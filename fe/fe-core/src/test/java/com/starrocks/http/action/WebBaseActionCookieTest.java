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

package com.starrocks.http.action;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.DefaultAuthorizationProvider;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.HttpAuthManager;
import com.starrocks.http.HttpAuthManager.SessionValue;
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
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * A Web UI request that carries a session cookie is checked against the context stored at login,
 * so the roles that come from the user's groups, and the roles on an ephemeral user's session,
 * count without authenticating the request again. The requests here carry no Authorization
 * header: falling back to it would answer 401.
 */
public class WebBaseActionCookieTest {

    private ConnectContext ctx;

    /** Records whether the page was served or the client was asked to authenticate again. */
    private static class RecordingAction extends WebBaseAction {
        boolean served;
        boolean authRequested;

        RecordingAction() {
            super(null);
        }

        @Override
        public void executeGet(BaseRequest request, BaseResponse response) {
            served = true;
        }

        @Override
        protected void writeAuthResponse(BaseRequest request, BaseResponse response) {
            authRequested = true;
        }
    }

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

    private RecordingAction requestWithSession(ConnectContext session) {
        String sessionId = UUID.randomUUID().toString();
        SessionValue value = new SessionValue();
        value.connectContext = session;
        HttpAuthManager.getInstance().addSessionValue(sessionId, value);

        DefaultHttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpHeaderNames.COOKIE, WebBaseAction.STARROCKS_SESSION_ID + "=" + sessionId);
        BaseRequest request = new BaseRequest(null,
                new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/system", headers)) {
            @Override
            public String getHostString() {
                return "127.0.0.1";
            }
        };
        RecordingAction action = new RecordingAction();
        action.execute(request, new BaseResponse());
        return action;
    }

    @Test
    public void testAdminThroughGroupIsServedFromTheCookie() throws Exception {
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("create user u_grp_admin", ctx), ctx);
        GrantRoleStmt grantRoleStmt = new GrantRoleStmt(List.of("db_admin", "user_admin"), "g_admin",
                GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(grantRoleStmt, ctx);
        GlobalStateMgr.getCurrentState().getAuthorizationMgr().grantRole(grantRoleStmt);

        ConnectContext session = new ConnectContext();
        session.setCurrentUserIdentity(new UserIdentity("u_grp_admin", "%"));
        session.setGroups(Set.of("g_admin"));
        session.setCurrentRoleIds(Set.of());

        RecordingAction action = requestWithSession(session);
        Assertions.assertTrue(action.served);
        Assertions.assertFalse(action.authRequested);
        Assertions.assertEquals(Set.of("g_admin"), ConnectContext.get().getGroups(),
                "the request's context carries the session's groups");
    }

    @Test
    public void testEphemeralUserSessionRolesAreServedFromTheCookie() {
        ConnectContext session = new ConnectContext();
        session.setCurrentUserIdentity(UserIdentity.createEphemeralUserIdent("eph_admin", "10.0.0.1"));
        session.setCurrentRoleIds(Set.of(PrivilegeBuiltinConstants.DB_ADMIN_ROLE_ID,
                PrivilegeBuiltinConstants.USER_ADMIN_ROLE_ID));

        RecordingAction action = requestWithSession(session);
        Assertions.assertTrue(action.served);
        Assertions.assertFalse(action.authRequested);
        Assertions.assertEquals(session.getCurrentRoleIds(), ConnectContext.get().getCurrentRoleIds(),
                "the request's context carries the session's roles");
    }

    @Test
    public void testUserWithoutAdminRoleIsAskedToAuthenticate() throws Exception {
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("create user u_plain", ctx), ctx);
        ConnectContext session = new ConnectContext();
        session.setCurrentUserIdentity(new UserIdentity("u_plain", "%"));
        session.setGroups(Set.of("g_none"));
        session.setCurrentRoleIds(Set.of());

        RecordingAction action = requestWithSession(session);
        Assertions.assertFalse(action.served);
        Assertions.assertTrue(action.authRequested);
    }
}
