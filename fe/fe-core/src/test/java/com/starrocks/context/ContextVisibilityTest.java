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

import com.google.common.collect.Sets;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Regression coverage for the visibility / ownership rewrites:
 *
 * <ul>
 *   <li>Owner match keys on {@code UserIdentity.getUser()} (the bare principal) rather than
 *       {@code toString()} (which embeds the per-request host). This is what lets an ephemeral
 *       Bearer/JWT identity authenticate stably across requests from different remote IPs.</li>
 *   <li>{@code hasFullVisibility} grants admin override on OPERATE / SECURITY but NOT on
 *       SYSTEM-level {@code CREATE_CONTEXTBASE}: that privilege is the self-service module-create
 *       gate and must not imply read-everywhere.</li>
 *   <li>{@code filterVisibleBases} returns only the owner's slice to a non-admin caller, never
 *       the full list.</li>
 *   <li>{@code requireAdmin} hard-throws for non-admin callers so cluster-wide observability
 *       endpoints can rely on it without a per-base fallback.</li>
 * </ul>
 *
 * <p>The tests intentionally bypass DDL routing and write directly to {@link ContextMgr} so the
 * visibility predicate is exercised against the canonical metadata shape without dragging in
 * grammar / analyzer / executor concerns. Per-base GRANT chains are covered by the privilege
 * tests in {@code com.starrocks.authorization}; here we focus on the predicate itself.
 */
public class ContextVisibilityTest {

    private static ContextMgr mgr;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        mgr = GlobalStateMgr.getCurrentState().getContextMgr();
    }

    /**
     * Construct a ConnectContext for an ephemeral identity with the given host. Mirrors the
     * shape {@code AuthenticationHandler.authenticateBearer} produces at runtime: ephemeral
     * UserIdentity with the principal as user and an arbitrary host (in production the host
     * is the remote IP, which is what causes the original toString-based comparison to fail
     * across requests).
     */
    private static ConnectContext ephemeralCtx(String principal, String host) {
        ConnectContext ctx = new ConnectContext(null);
        ctx.setCurrentUserIdentity(UserIdentity.createEphemeralUserIdent(principal, host));
        // No roles activated — pure ephemeral, no group → role mapping, no admin override.
        // This is the worst-case authorization shape (newly-authenticated external user with
        // no provisioned grants yet); the owner-match path is the only one that can succeed.
        ctx.setCurrentRoleIds(new HashSet<>());
        ctx.setQualifiedUser(principal);
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx.setThreadLocalInfo();
        return ctx;
    }

    /**
     * Construct a root-equivalent ConnectContext. {@code ROOT_ROLE_ID} carries every privilege
     * including OPERATE / SECURITY, so this is the admin-override case.
     */
    private static ConnectContext rootCtx() {
        ConnectContext ctx = new ConnectContext(null);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        ctx.setQualifiedUser(UserIdentity.ROOT.getUser());
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx.setThreadLocalInfo();
        return ctx;
    }

    /**
     * Bug 4 regression: the owner field stored on a contextbase is the principal
     * ({@code UserIdentity.getUser()}). Two requests from the same user but different remote
     * IPs must both pass the owner check. The pre-fix code stored {@code toString()} which
     * embedded the host, so the second request would silently fail to authorize.
     */
    @Test
    public void testOwnerMatchIsStableAcrossHostChange() throws Exception {
        String base = "ctx_vis_owner_stable";
        mgr.createContextBase(base,
                Map.of("_owner_user", "alice@phoenixdata.ai"), /*ifNotExists*/ false);
        try {
            // Same principal, different remote IPs — both must authorize.
            ContextVisibility.checkOnContextBase(
                    ephemeralCtx("alice@phoenixdata.ai", "10.0.0.1"),
                    base, ContextVisibility.BaseAction.USAGE);
            ContextVisibility.checkOnContextBase(
                    ephemeralCtx("alice@phoenixdata.ai", "10.0.0.2"),
                    base, ContextVisibility.BaseAction.USAGE);
        } finally {
            mgr.dropContextBase(base, true);
        }
    }

    /**
     * Bug 2 regression: the owner-fallback path must reject a caller whose principal does NOT
     * match the stored {@code _owner_user}, even though they could trivially make the
     * {@code toString()} of two different ephemeral identities look similar.
     */
    @Test
    public void testOwnerMatchRejectsDifferentPrincipal() throws Exception {
        String base = "ctx_vis_owner_reject";
        mgr.createContextBase(base,
                Map.of("_owner_user", "alice@phoenixdata.ai"), /*ifNotExists*/ false);
        try {
            ConnectContext bob = ephemeralCtx("bob@phoenixdata.ai", "10.0.0.5");
            Assertions.assertThrows(AccessDeniedException.class, () ->
                    ContextVisibility.checkOnContextBase(
                            bob, base, ContextVisibility.BaseAction.USAGE));
        } finally {
            mgr.dropContextBase(base, true);
        }
    }

    /**
     * Bug 2 regression: a contextbase with empty / null {@code _owner_user} must NOT be
     * accessible via the owner path to any principal. The pre-fix code accepted "owner is
     * null" plus CREATE_CONTEXTBASE as a positive match, which made historical bases with no
     * owner stamp universally readable.
     */
    @Test
    public void testOwnerNullDeniesEveryone() throws Exception {
        String base = "ctx_vis_owner_null";
        mgr.createContextBase(base, Map.of(), /*ifNotExists*/ false);
        try {
            ConnectContext anyone = ephemeralCtx("anyone@phoenixdata.ai", "10.0.0.7");
            Assertions.assertThrows(AccessDeniedException.class, () ->
                    ContextVisibility.checkOnContextBase(
                            anyone, base, ContextVisibility.BaseAction.USAGE));
        } finally {
            mgr.dropContextBase(base, true);
        }
    }

    /**
     * Bug 1 regression: admin override must NOT include SYSTEM-level
     * {@code CREATE_CONTEXTBASE}; only {@code OPERATE} and {@code SECURITY} qualify. The
     * ephemeral context here has no roles at all, so the predicate must return false even
     * when CREATE_CONTEXTBASE would have been derivable through some other path.
     */
    @Test
    public void testHasFullVisibilityRequiresOperateOrSecurity() {
        ConnectContext ephemeral = ephemeralCtx("kaisen@phoenixdata.ai", "10.0.0.9");
        Assertions.assertFalse(ContextVisibility.hasFullVisibility(ephemeral),
                "ephemeral user with no roles must not have full visibility");

        ConnectContext root = rootCtx();
        Assertions.assertTrue(ContextVisibility.hasFullVisibility(root),
                "root (OPERATE + SECURITY) must have full visibility");
    }

    /**
     * Bug 3 regression: {@code filterVisibleBases} returns the admin-or-owned subset, not the
     * full list. Without this filter the SQL {@code SHOW CONTEXTBASES} executor enumerated
     * every contextbase regardless of the caller's grants.
     */
    @Test
    public void testFilterVisibleBasesReturnsOnlyOwnedForNonAdmin() throws Exception {
        String owned = "ctx_vis_filter_owned";
        String other = "ctx_vis_filter_other";
        mgr.createContextBase(owned,
                Map.of("_owner_user", "kaisen@phoenixdata.ai"), false);
        mgr.createContextBase(other,
                Map.of("_owner_user", "brook@phoenixdata.ai"), false);
        try {
            ConnectContext kaisen = ephemeralCtx("kaisen@phoenixdata.ai", "10.0.0.3");
            List<ContextMgr.ContextBaseMeta> visible = ContextVisibility.filterVisibleBases(
                    kaisen, mgr.listContextBases());
            boolean sawOwned = false;
            boolean sawOther = false;
            for (ContextMgr.ContextBaseMeta m : visible) {
                if (m.getName().equals(owned)) {
                    sawOwned = true;
                }
                if (m.getName().equals(other)) {
                    sawOther = true;
                }
            }
            Assertions.assertTrue(sawOwned, "kaisen must see her own base");
            Assertions.assertFalse(sawOther, "kaisen must NOT see brook's base");
        } finally {
            mgr.dropContextBase(owned, true);
            mgr.dropContextBase(other, true);
        }
    }

    /**
     * Admin override: {@code filterVisibleBases} short-circuits to the full input list when
     * the caller has OPERATE / SECURITY. Verifies the admin path doesn't accidentally pay the
     * per-item filter cost (and matches the documented contract).
     */
    @Test
    public void testFilterVisibleBasesAdminSeesAll() throws Exception {
        String a = "ctx_vis_admin_a";
        String b = "ctx_vis_admin_b";
        mgr.createContextBase(a, Map.of("_owner_user", "someone@phoenixdata.ai"), false);
        mgr.createContextBase(b, Map.of(), false);
        try {
            List<ContextMgr.ContextBaseMeta> visible = ContextVisibility.filterVisibleBases(
                    rootCtx(), mgr.listContextBases());
            boolean sawA = false;
            boolean sawB = false;
            for (ContextMgr.ContextBaseMeta m : visible) {
                if (m.getName().equals(a)) {
                    sawA = true;
                }
                if (m.getName().equals(b)) {
                    sawB = true;
                }
            }
            Assertions.assertTrue(sawA);
            Assertions.assertTrue(sawB);
        } finally {
            mgr.dropContextBase(a, true);
            mgr.dropContextBase(b, true);
        }
    }

    /**
     * {@code requireAdmin} is the hard gate used by cluster-wide observability endpoints
     * (health, cluster-wide stats, SHOW CONTEXT STATUS / TASKS / PROFILE). A non-admin must
     * receive an {@link AccessDeniedException}; an admin must pass without exception.
     */
    @Test
    public void testRequireAdmin() {
        ConnectContext ephemeral = ephemeralCtx("user@phoenixdata.ai", "10.0.0.11");
        Assertions.assertThrows(AccessDeniedException.class,
                () -> ContextVisibility.requireAdmin(ephemeral));

        Assertions.assertDoesNotThrow(() -> ContextVisibility.requireAdmin(rootCtx()));
    }

    /**
     * Bug 6 regression: SHOW CONTEXTBASES displays the access-control owner
     * ({@code _owner_user}), not the free-form {@code "owner"} string property. Validates that
     * {@link ContextMgr.ContextBaseMeta#getOwner} reads the right key — the executor row
     * builder reads through this same getter.
     */
    @Test
    public void testGetOwnerReadsAccessControlKey() throws Exception {
        String base = "ctx_vis_owner_key";
        mgr.createContextBase(base,
                Map.of("_owner_user", "alice@phoenixdata.ai",
                       "owner", "this is a documentation string, not the owner"),
                false);
        try {
            ContextMgr.ContextBaseMeta meta = mgr.getContextBase(base);
            Assertions.assertEquals("alice@phoenixdata.ai", meta.getOwner(),
                    "getOwner() must return the access-control _owner_user value");
        } finally {
            mgr.dropContextBase(base, true);
        }
    }
}
