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

package com.starrocks.authentication;

import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.DefaultAuthorizationProvider;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ExecuteAsExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.parser.NodePosition;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

public class ExecuteAsExecutorTest {
    private AuthenticationMgr authenticationMgr;
    private AuthorizationMgr authorizationMgr;

    @BeforeEach
    public void setUp() throws Exception {
        // Mock EditLog
        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);

        new MockUp<LDAPGroupProvider>() {
            @Mock
            public void init() throws DdlException {
                // do nothing
            }
        };

        Map<String, String> properties = new HashMap<>();
        properties.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "ldap");
        properties.put(LDAPGroupProvider.LDAP_USER_SEARCH_ATTR, "uid");

        String groupName = "ldap_group_provider";
        authenticationMgr.replayCreateGroupProvider(groupName, properties);
        Config.group_provider = new String[] {groupName};
        LDAPGroupProvider ldapGroupProvider = (LDAPGroupProvider) authenticationMgr.getGroupProvider(groupName);

        Map<String, Set<String>> groups = new HashMap<>();
        groups.put("impersonate_user", Set.of("group1", "group2"));
        groups.put("u1", Set.of("group3"));
        groups.put("u2", Set.of("group4"));
        ldapGroupProvider.setUserToGroupCache(groups);
    }

    @Test
    public void testExecuteAs() throws Exception {
        authorizationMgr.createRole(new CreateRoleStmt(List.of("r1"), true, ""));
        authorizationMgr.createRole(new CreateRoleStmt(List.of("r2"), true, ""));

        authenticationMgr.createUser(
                new CreateUserStmt(new UserRef("impersonate_user", "%"), true, null, List.of(), Map.of(), NodePosition.ZERO));
        authenticationMgr.createUser(
                new CreateUserStmt(new UserRef("u1", "%"), true, null, List.of("r1"), Map.of(), NodePosition.ZERO));
        authenticationMgr.createUser(
                new CreateUserStmt(new UserRef("u2", "%"), true, null, List.of("r2"), Map.of(), NodePosition.ZERO));

        long roleId1 = authorizationMgr.getRoleIdByNameAllowNull("r1");
        long roleId2 = authorizationMgr.getRoleIdByNameAllowNull("r2");

        // login as impersonate_user

        ConnectContext context = new ConnectContext();
        AuthenticationHandler.authenticate(context, "impersonate_user", "%", MysqlPassword.EMPTY_PASSWORD);

        Assertions.assertEquals("impersonate_user", context.getAuthenticationContext().getQualifiedUser());
        Assertions.assertEquals(Set.of("group1", "group2"), context.getGroups());

        ExecuteAsStmt executeAsStmt = new ExecuteAsStmt(new UserRef("u1", "%"), false);
        ExecuteAsExecutor.execute(executeAsStmt, context);
        Assertions.assertEquals(Set.of("group3"), context.getGroups());
        Assertions.assertEquals(Set.of(roleId1), context.getCurrentRoleIds());

        ExecuteAsStmt executeAsStmt2 = new ExecuteAsStmt(new UserRef("u2", "%"), false);
        ExecuteAsExecutor.execute(executeAsStmt2, context);
        Assertions.assertEquals(Set.of("group4"), context.getGroups());
        Assertions.assertEquals(Set.of(roleId2), context.getCurrentRoleIds());
    }
}
