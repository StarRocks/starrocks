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

package com.starrocks.context;

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Canonical authorization helpers for the semantic-context module. Shared by the REST surface
 * ({@code com.starrocks.http.rest.context.ContextRestAuth}) and the SQL surface
 * ({@code com.starrocks.qe.ShowExecutor}, {@code AuthorizerStmtVisitor}) so both transports apply
 * the exact same predicate. Previously the REST helpers lived inside the REST package and the SQL
 * SHOW path bypassed them entirely, which left {@code SHOW CONTEXTBASES} unfiltered and the two
 * surfaces semantically divergent.
 *
 * <p>Predicates implemented here:
 * <ul>
 *   <li>{@link #hasFullVisibility} — admin override via SYSTEM-level {@code OPERATE} or
 *       {@code SECURITY}. Notably NOT {@code CREATE_CONTEXTBASE}: that privilege is the
 *       module-create gate (anyone provisioned with self-service creation can hold it) and must
 *       not imply READ-ALL on every other base.</li>
 *   <li>{@link #checkOnContextBase} — admin override → per-base GRANT
 *       ({@code USAGE|ALTER|DROP ON CONTEXTBASE &lt;name&gt;}) → owner match. The owner check
 *       compares {@code UserIdentity.getUser()} (the principal/email) against the stored
 *       {@code _owner_user} so ephemeral identities authenticate stably across requests from
 *       different remote IPs.</li>
 *   <li>{@link #filterVisibleBases} / {@link #filterVisibleCollections} /
 *       {@link #filterVisibleWorkspaces} — list-style filters that return either the full input
 *       (admin override) or the subset the caller has USAGE on, used by both list endpoints and
 *       {@code SHOW CONTEXT *} so non-admin users see their own slice rather than 401-ing or
 *       seeing the full topology.</li>
 *   <li>{@link #requireAdmin} — hard admin-only gate for cluster-wide observability endpoints
 *       ({@code SHOW CONTEXT STATUS / TASKS / PROFILE}, {@code /api/context/health},
 *       cluster-wide {@code /api/context/stats}) where the returned data is aggregate across
 *       every base and there is no per-base scope to filter to.</li>
 * </ul>
 */
public final class ContextVisibility {

    private ContextVisibility() {
    }

    public enum BaseAction {
        USAGE,
        ALTER,
        DROP
    }

    /**
     * Returns true when the caller holds an admin override that bypasses per-base ACLs.
     * Restricted to system-level {@code OPERATE} and {@code SECURITY}; {@code CREATE_CONTEXTBASE}
     * is deliberately excluded so a user provisioned with self-service base creation cannot
     * enumerate other tenants' data.
     */
    public static boolean hasFullVisibility(ConnectContext ctx) {
        try {
            Authorizer.checkSystemAction(ctx, PrivilegeType.OPERATE);
            return true;
        } catch (AccessDeniedException ignored) {
            // fall through
        }
        try {
            Authorizer.checkSystemAction(ctx, PrivilegeType.SECURITY);
            return true;
        } catch (AccessDeniedException ignored) {
            // fall through
        }
        return false;
    }

    /**
     * Hard admin-only gate. Throws when the caller is not a system admin
     * ({@code OPERATE} or {@code SECURITY}). Used by cluster-wide observability endpoints whose
     * payload aggregates across every contextbase and cannot meaningfully be filtered to a
     * per-caller subset.
     */
    public static void requireAdmin(ConnectContext ctx) throws AccessDeniedException {
        try {
            Authorizer.checkSystemAction(ctx, PrivilegeType.OPERATE);
            return;
        } catch (AccessDeniedException ignored) {
            // fall through
        }
        Authorizer.checkSystemAction(ctx, PrivilegeType.SECURITY);
    }

    /**
     * Authorize an operation on a named contextbase. The three accepted paths, in order:
     * <ol>
     *   <li>Admin override ({@code OPERATE} / {@code SECURITY} on SYSTEM).</li>
     *   <li>Per-base GRANT for the required action ({@code USAGE} for reads / content mutation /
     *       collection / workspace ops, {@code ALTER} for {@code ALTER CONTEXTBASE},
     *       {@code DROP} for {@code DROP CONTEXTBASE}).</li>
     *   <li>Owner match: the caller's principal (from {@code UserIdentity.getUser()}) equals the
     *       stored {@code _owner_user}. Owner with empty/null {@code _owner_user} does NOT
     *       match — historical bases without a recorded owner must go through per-base GRANT.</li>
     * </ol>
     * The previous code permitted a fourth fallback ("system {@code CREATE_CONTEXTBASE} plus
     * matching owner OR null owner") that conflated module-create with read-anywhere; that
     * path is intentionally removed here.
     */
    public static void checkOnContextBase(ConnectContext ctx, String contextBaseName, BaseAction action)
            throws AccessDeniedException {
        // 1. Admin override.
        if (hasFullVisibility(ctx)) {
            return;
        }
        if (contextBaseName == null || contextBaseName.isEmpty()) {
            // Non-admin path requires a concrete base name — the caller's authority can only be
            // evaluated against a specific contextbase grant or owner record.
            throw new AccessDeniedException("contextbase name is required for non-admin callers");
        }
        // 2. Per-base privilege grant.
        PrivilegeType perBase;
        switch (action) {
            case ALTER:
                perBase = PrivilegeType.ALTER;
                break;
            case DROP:
                perBase = PrivilegeType.DROP;
                break;
            case USAGE:
            default:
                perBase = PrivilegeType.USAGE;
                break;
        }
        try {
            Authorizer.checkContextBaseAction(ctx, contextBaseName, perBase);
            return;
        } catch (AccessDeniedException ignored) {
            // fall through to ownership check
        }
        // 3. Owner match (principal-only comparison; never includes host).
        ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
        ContextMgr.ContextBaseMeta meta = mgr.getContextBase(contextBaseName);
        String owner = meta == null ? null : meta.getOwner();
        String me = ctx.getCurrentUserIdentity() == null ? null : ctx.getCurrentUserIdentity().getUser();
        if (owner != null && !owner.isEmpty() && owner.equals(me)) {
            return;
        }
        throw new AccessDeniedException(
                "no privilege on contextbase '" + contextBaseName + "' for action " + action);
    }

    /**
     * Returns true when {@code ctx} satisfies {@link #checkOnContextBase} for USAGE on the named
     * base. Swallows {@link AccessDeniedException} as the negative answer — used by list
     * endpoints and SHOW filters where "can't see" is the expected normal outcome for non-owners.
     */
    public static boolean canSeeContextBase(ConnectContext ctx, String contextBaseName) {
        try {
            checkOnContextBase(ctx, contextBaseName, BaseAction.USAGE);
            return true;
        } catch (AccessDeniedException ignored) {
            return false;
        }
    }

    /**
     * Returns true when {@code ctx} has USAGE on at least one contextbase (admin paths
     * short-circuit). Used by the basic-readiness endpoint so any module tenant can probe leader
     * status without operator-level system privileges.
     */
    public static boolean hasAnyContextBaseUsage(ConnectContext ctx) {
        if (hasFullVisibility(ctx)) {
            return true;
        }
        ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
        for (ContextMgr.ContextBaseMeta meta : mgr.listContextBases()) {
            if (canSeeContextBase(ctx, meta.getName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Filter contextbases to those the caller can read. Admin paths short-circuit to the full
     * input list (avoids per-item authorizer calls when the caller has OPERATE/SECURITY).
     */
    public static List<ContextMgr.ContextBaseMeta> filterVisibleBases(
            ConnectContext ctx, Collection<ContextMgr.ContextBaseMeta> metas) {
        if (hasFullVisibility(ctx)) {
            return new ArrayList<>(metas);
        }
        List<ContextMgr.ContextBaseMeta> visible = new ArrayList<>();
        for (ContextMgr.ContextBaseMeta m : metas) {
            if (canSeeContextBase(ctx, m.getName())) {
                visible.add(m);
            }
        }
        return visible;
    }

    /**
     * Filter collections to those whose owning contextbase the caller can read. Resolves each
     * collection's owning base via {@code getContextBaseById} so a caller without that base's
     * USAGE never learns the collection exists.
     */
    public static List<ContextMgr.CollectionMeta> filterVisibleCollections(
            ConnectContext ctx, Collection<ContextMgr.CollectionMeta> metas) {
        if (hasFullVisibility(ctx)) {
            return new ArrayList<>(metas);
        }
        ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
        List<ContextMgr.CollectionMeta> visible = new ArrayList<>();
        for (ContextMgr.CollectionMeta m : metas) {
            ContextMgr.ContextBaseMeta cb = mgr.getContextBaseById(m.getContextBaseId());
            if (cb != null && canSeeContextBase(ctx, cb.getName())) {
                visible.add(m);
            }
        }
        return visible;
    }

    /**
     * Filter workspaces to those whose owning contextbase the caller can read. Workspace names
     * follow {@code <contextbase>.<workspace>}, so the prefix preceding the first {@code '.'}
     * is the visibility key.
     */
    public static List<ContextMgr.WorkspaceMeta> filterVisibleWorkspaces(
            ConnectContext ctx, Collection<ContextMgr.WorkspaceMeta> metas) {
        if (hasFullVisibility(ctx)) {
            return new ArrayList<>(metas);
        }
        List<ContextMgr.WorkspaceMeta> visible = new ArrayList<>();
        for (ContextMgr.WorkspaceMeta m : metas) {
            String qualifiedName = m.getName();
            if (qualifiedName == null) {
                continue;
            }
            int dot = qualifiedName.indexOf('.');
            if (dot <= 0) {
                continue;
            }
            String baseName = qualifiedName.substring(0, dot);
            if (canSeeContextBase(ctx, baseName)) {
                visible.add(m);
            }
        }
        return visible;
    }

    /**
     * Resolve the contextbase id back to a base name. Convenience wrapper around
     * {@code ContextMgr.getContextBaseById} for callers that already have the id from a row but
     * not the name — used by the SHOW COLLECTIONS executor so unscoped {@code SHOW CONTEXT
     * COLLECTIONS} can print the actual base each row belongs to instead of a literal empty
     * string.
     */
    public static String resolveContextBaseName(long contextBaseId) {
        ContextMgr.ContextBaseMeta meta = GlobalStateMgr.getCurrentState().getContextMgr()
                .getContextBaseById(contextBaseId);
        return meta == null ? "" : meta.getName();
    }

    /**
     * Extract the {@code <contextbase>} prefix from a qualified workspace name
     * ({@code <contextbase>.<workspace>}). Returns empty string when the name does not contain
     * a separator. Mirrors the prefix-extraction logic used by
     * {@link #filterVisibleWorkspaces} so executor code rendering rows can recover the
     * displayable contextbase without a second lookup.
     */
    public static String workspaceContextBasePrefix(String qualifiedWorkspaceName) {
        if (qualifiedWorkspaceName == null) {
            return "";
        }
        int dot = qualifiedWorkspaceName.indexOf('.');
        if (dot <= 0) {
            return "";
        }
        return qualifiedWorkspaceName.substring(0, dot);
    }
}
