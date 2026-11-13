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

package com.starrocks.http.rest;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.DefaultAuthorizationProvider;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.http.BaseRequest;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.GrantType;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

/**
 * The management endpoints admit a user that holds the admin roles: granted to it, to one of its
 * groups, or to a role it holds; for an ephemeral user (one logged in through a security
 * integration), on its session. The endpoints used to look only at the roles granted to the user.
 * {@link StorageTypeCheckAction} stands for them: it
 * checks the admin role first and then fails on the missing db parameter, so a DdlException means
 * the check let the request through.
 */
public class AdminRoleCheckTest {

    private ConnectContext ctx;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        ctx = new ConnectContext();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(
                new AuthorizationMgr(new DefaultAuthorizationProvider()));
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(new AuthenticationMgr());
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
        UtFrameUtils.tearDownForPersisTest();
    }

    private void callEndpointAs(ConnectContext session) throws Exception {
        session.setThreadLocalInfo();
        BaseRequest request = new BaseRequest(null, new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/api/_check_storagetype", new DefaultHttpHeaders()));
        new StorageTypeCheckAction(null).executeWithoutPassword(request, null);
    }

    private void assertAdmitted(ConnectContext session) {
        DdlException e = Assertions.assertThrows(DdlException.class, () -> callEndpointAs(session));
        Assertions.assertTrue(e.getMessage().contains("Parameter db is missing"), e.getMessage());
    }

    @Test
    public void testEphemeralAdminIsAdmitted() {
        ctx.setCurrentUserIdentity(UserIdentity.createEphemeralUserIdent("eph_admin", "10.0.0.1"));
        ctx.setCurrentRoleIds(Set.of(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        assertAdmitted(ctx);
    }

    @Test
    public void testAdminThroughGroupIsAdmitted() throws Exception {
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("create user u_grp_admin", ctx), ctx);
        GrantRoleStmt grantRoleStmt = new GrantRoleStmt(List.of("db_admin", "user_admin"), "g_admin",
                GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(grantRoleStmt, ctx);
        AuthorizationMgr authorizationMgr = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        authorizationMgr.grantRole(grantRoleStmt);

        ctx.setCurrentUserIdentity(new UserIdentity("u_grp_admin", "%"));
        ctx.setGroups(Set.of("g_admin"));
        // As after SET ROLE NONE: the session's roles no longer list the group roles, which stay active.
        ctx.setCurrentRoleIds(Set.of());
        assertAdmitted(ctx);
    }

    private void execute(String sql) throws Exception {
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);
    }

    @Test
    public void testAdminThroughAnInheritingRoleIsAdmitted() throws Exception {
        execute("create user u_ops");
        execute("create role r_ops");
        execute("grant db_admin, user_admin to role r_ops");
        execute("grant r_ops to user u_ops");

        ctx.setCurrentUserIdentity(new UserIdentity("u_ops", "%"));
        ctx.setCurrentRoleIds(Set.of());
        assertAdmitted(ctx);
    }

    @Test
    public void testAdminRoleGrantedButNotActivatedIsAdmitted() throws Exception {
        execute("create user u_direct_admin");
        execute("grant db_admin, user_admin to user u_direct_admin");

        // not among the default roles, so not activated at login: it still counts, as it always has
        ctx.setCurrentUserIdentity(new UserIdentity("u_direct_admin", "%"));
        ctx.setCurrentRoleIds(Set.of());
        assertAdmitted(ctx);
    }

    @Test
    public void testAdminRoleRevokedAfterLoginIsRefused() throws Exception {
        execute("create user u_revoked");

        // the session still lists the roles activated at login, but the user no longer has them
        ctx.setCurrentUserIdentity(new UserIdentity("u_revoked", "%"));
        ctx.setCurrentRoleIds(Set.of(PrivilegeBuiltinConstants.DB_ADMIN_ROLE_ID,
                PrivilegeBuiltinConstants.USER_ADMIN_ROLE_ID));
        Assertions.assertThrows(AccessDeniedException.class, () -> callEndpointAs(ctx));
    }

    @Test
    public void testNonAdminIsRefused() throws Exception {
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("create user u_plain", ctx), ctx);
        ctx.setCurrentUserIdentity(new UserIdentity("u_plain", "%"));
        ctx.setGroups(Set.of("g_none"));
        ctx.setCurrentRoleIds(Set.of());
        Assertions.assertThrows(AccessDeniedException.class, () -> callEndpointAs(ctx));
    }
}
