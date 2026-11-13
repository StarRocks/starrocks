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

package com.starrocks.authorization;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.epack.authorization.AuthorizationMgrEPack;
import com.starrocks.epack.authorization.AuthorizationProviderEPack;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * The enterprise lookup of an ephemeral user's roles. Such a user, one logged in through a
 * security integration, has no entry in the privilege metadata: its roles live only in its own
 * session. The lookup must answer from that session and refuse anywhere else, instead of throwing
 * an NPE on a thread without a session or returning the roles of whoever owns the current session.
 */
public class EphemeralUserRoleLookupTest {
    private static final String DB = "ephemeral_role_lookup_db";
    private static final String TABLE = "tbl1";
    private static final String ROLE = "ephemeral_role_lookup_role";

    private static ConnectContext rootCtx;
    private static AuthorizationMgr authorizationMgr;
    private static long roleId;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        rootCtx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        rootCtx.setThreadLocalInfo();

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        globalStateMgr.setAuthenticationMgr(new AuthenticationMgr());
        authorizationMgr = new AuthorizationMgrEPack(new AuthorizationProviderEPack());
        globalStateMgr.setAuthorizationMgr(authorizationMgr);

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("create role " + ROLE, rootCtx), rootCtx);
        roleId = authorizationMgr.getRoleIdByNameAllowNull(ROLE);

        new StarRocksAssert(rootCtx).withDatabase(DB).useDatabase(DB)
                .withTable("create table " + TABLE + " (id int) properties('replication_num'='1')");
    }

    @AfterAll
    public static void tearDown() throws Exception {
        rootCtx.setThreadLocalInfo();
        new StarRocksAssert(rootCtx).dropDatabase(DB);
    }

    @BeforeEach
    public void restoreRootSession() {
        rootCtx.setThreadLocalInfo();
    }

    private static UserIdentity ephemeralUser() {
        return UserIdentity.createEphemeralUserIdent("ephemeral_role_lookup_user", "%");
    }

    @Test
    public void testLookupFailsOnThreadWithoutSession() {
        // An FE RPC handler or a task thread before it installs a session.
        ConnectContext.remove();
        PrivilegeException e = Assertions.assertThrows(PrivilegeException.class,
                () -> authorizationMgr.getRoleIdsByUser(ephemeralUser()));
        Assertions.assertTrue(e.getMessage().contains("outside its own session"), e.getMessage());
    }

    @Test
    public void testLookupFailsInAnotherUsersSession() {
        // root's session is on the thread: its roles must not be reported as the ephemeral user's.
        Assertions.assertThrows(PrivilegeException.class, () -> authorizationMgr.getRoleIdsByUser(ephemeralUser()));
    }

    @Test
    public void testLookupAnswersFromOwnSession() throws Exception {
        ConnectContext own = new ConnectContext();
        own.setCurrentUserIdentity(ephemeralUser());
        Set<Long> sessionRoles = new HashSet<>(Set.of(roleId));
        own.setCurrentRoleIds(sessionRoles);
        own.setThreadLocalInfo();

        Set<Long> roles = authorizationMgr.getRoleIdsByUser(ephemeralUser());
        Assertions.assertEquals(Set.of(roleId), roles);
        // The caller gets a copy: changing it leaves the session alone.
        roles.clear();
        Assertions.assertEquals(Set.of(roleId), own.getCurrentRoleIds());
    }

    @Test
    public void testTablesQueryFallsBackToBeScanWithNonEqualityPredicate() throws Exception {
        // information_schema.tables is answered by the FE in the session thread only for equality
        // predicates; any other predicate, and information_schema.columns always, is scanned by the
        // BE, which calls the FE back over RPC on a thread without a session.
        String onFe = UtFrameUtils.getFragmentPlan(rootCtx, "select table_name from information_schema.tables"
                + " where table_schema = '" + DB + "' and table_name = '" + TABLE + "'");
        Assertions.assertFalse(onFe.contains("SCAN SCHEMA"), onFe);

        String onBe = UtFrameUtils.getFragmentPlan(rootCtx, "select table_name from information_schema.tables"
                + " where table_schema = '" + DB + "' and table_name like 'tbl%'");
        Assertions.assertTrue(onBe.contains("SCAN SCHEMA"), onBe);

        String columns = UtFrameUtils.getFragmentPlan(rootCtx, "select column_name from information_schema.columns"
                + " where table_schema = '" + DB + "' and table_name = '" + TABLE + "'");
        Assertions.assertTrue(columns.contains("SCAN SCHEMA"), columns);
    }
}
