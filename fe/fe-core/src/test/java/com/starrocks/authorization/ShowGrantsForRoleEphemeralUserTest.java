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
import com.starrocks.common.ErrorReportException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.GrantType;
import com.starrocks.sql.ast.ShowGrantsStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

public class ShowGrantsForRoleEphemeralUserTest {

    @BeforeEach
    public void setUpPersistJournal() throws Exception {
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDownPersistJournal() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testShowGrantsForRoleAsEphemeralUser() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        AuthorizationMgr authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(new AuthenticationMgr());

        for (String role : List.of("r_ops", "r_other")) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("create role " + role, ctx), ctx);
        }
        authorizationMgr.grantRole(new GrantRoleStmt(List.of("r_ops"), "g_ops", GrantType.GROUP, NodePosition.ZERO));

        // Log in the way a security integration does: an ephemeral user whose roles come from its group.
        UserIdentity jwtUser = UserIdentity.createEphemeralUserIdent("jwt_user", "%");
        ctx.setCurrentUserIdentity(jwtUser);
        ctx.setGroups(Set.of("g_ops"));
        ctx.setCurrentRoleIds(jwtUser, Set.of("g_ops"));

        // A role the user holds needs no GRANT (threw 5203 before the fix).
        Authorizer.check(new ShowGrantsStmt("r_ops", GrantType.ROLE, NodePosition.ZERO), ctx);
        // Another role still needs GRANT.
        Assertions.assertThrows(ErrorReportException.class,
                () -> Authorizer.check(new ShowGrantsStmt("r_other", GrantType.ROLE, NodePosition.ZERO), ctx));

        // With user_admin through the group, any role is allowed (threw 5203 before the fix).
        authorizationMgr.grantRole(
                new GrantRoleStmt(List.of("user_admin"), "g_ops", GrantType.GROUP, NodePosition.ZERO));
        ctx.setCurrentRoleIds(jwtUser, Set.of("g_ops"));
        Authorizer.check(new ShowGrantsStmt("r_other", GrantType.ROLE, NodePosition.ZERO), ctx);
    }
}
