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

import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class AuthenticationHandlerTest {

    @BeforeAll
    public static void setUpPersistJournal() throws Exception {
        // Real EditLog on an auto-committing pseudo journal (shields BDB): journal writes complete so the
        // WALApplier.apply() inside logJsonObject() still runs and the DDL takes effect in memory.
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterAll
    public static void tearDownPersistJournal() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @BeforeEach
    public void setUp() throws Exception {

        new MockUp<LDAPGroupProvider>() {
            @Mock
            public void init() throws DdlException {
                // do nothing
            }
        };
    }

    @Test
    public void testNativeReauthenticationDoesNotInheritPreviousDN() throws Exception {
        AuthenticationMgr previousManager = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        String[] previousProviders = Config.group_provider;
        try {
            GlobalStateMgr.getCurrentState().setAuthenticationMgr(new AuthenticationMgr());
            Config.group_provider = new String[0];
            ConnectContext context = new ConnectContext();
            context.setDistinguishedName("uid=previous_user,ou=people,dc=example,dc=com");

            AuthenticationHandler.authenticate(context, "root", "127.0.0.1", new byte[0]);

            Assertions.assertEquals("root", context.getDistinguishedName());
        } finally {
            Config.group_provider = previousProviders;
            GlobalStateMgr.getCurrentState().setAuthenticationMgr(previousManager);
        }
    }

    @Test
    public void testFailedReauthenticationPreservesDNButNextLoginReplacesIt() throws Exception {
        AuthenticationMgr previousManager = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        String[] previousProviders = Config.group_provider;
        String[] previousChain = Config.authentication_chain;
        try {
            GlobalStateMgr.getCurrentState().setAuthenticationMgr(new AuthenticationMgr());
            Config.group_provider = new String[0];
            Config.authentication_chain = new String[0];
            ConnectContext context = new ConnectContext();
            String previousDN = "uid=previous_user,ou=people,dc=example,dc=com";
            context.setDistinguishedName(previousDN);

            Assertions.assertThrows(AuthenticationException.class,
                    () -> AuthenticationHandler.authenticate(context, "missing_reauthentication_user",
                            "127.0.0.1", new byte[0]));
            Assertions.assertEquals(previousDN, context.getDistinguishedName());
            AuthenticationHandler.authenticate(context, "root", "127.0.0.1", new byte[0]);
            Assertions.assertEquals("root", context.getDistinguishedName());
        } finally {
            Config.group_provider = previousProviders;
            Config.authentication_chain = previousChain;
            GlobalStateMgr.getCurrentState().setAuthenticationMgr(previousManager);
        }
    }

    @Test
    public void testFailedReauthenticationPreservesAuthenticatedTaskCreator() throws Exception {
        AuthenticationMgr previousManager = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        String[] previousProviders = Config.group_provider;
        String[] previousChain = Config.authentication_chain;
        boolean previousAuthCheck = Config.enable_auth_check;
        try {
            GlobalStateMgr.getCurrentState().setAuthenticationMgr(new AuthenticationMgr());
            Config.group_provider = new String[0];
            Config.authentication_chain = new String[0];
            Config.enable_auth_check = true;
            ConnectContext context = new ConnectContext();
            AuthenticationHandler.authenticate(context, "root", "127.0.0.1", new byte[0]);
            TaskExecutionIdentity identity = TaskExecutionIdentity.capture(context);

            Assertions.assertThrows(AuthenticationException.class,
                    () -> AuthenticationHandler.authenticate(context, "missing_task_creator",
                            "127.0.0.1", new byte[0]));

            Assertions.assertSame(identity, TaskExecutionIdentity.capture(context));
        } finally {
            Config.enable_auth_check = previousAuthCheck;
            Config.group_provider = previousProviders;
            Config.authentication_chain = previousChain;
            GlobalStateMgr.getCurrentState().setAuthenticationMgr(previousManager);
        }
    }

    @Test
    public void testFailedIntegrationCannotSupplyDNForTheNextIntegration() throws Exception {
        String[] previousChain = Config.authentication_chain;
        try {
            Config.authentication_chain = new String[] {"task_failed_ldap", "task_successful_ldap"};
            new MockUp<AuthenticationMgr>() {
                @Mock
                public SecurityIntegration getSecurityIntegration(String name) {
                    return new SecurityIntegration(name, Map.of("type", "AUTHENTICATION_LDAP_SIMPLE")) {
                        @Override
                        public AuthenticationProvider getAuthenticationProvider() {
                            return (authContext, identity, response) -> {
                                if (name.equals("task_failed_ldap")) {
                                    authContext.setDistinguishedName("uid=failed_attempt,dc=example");
                                    throw new AuthenticationException("Rejected test provider");
                                }
                            };
                        }
                    };
                }
            };
            ConnectContext context = new ConnectContext();
            context.setAuthPlugin(AuthPlugin.Client.MYSQL_CLEAR_PASSWORD.toString());
            AuthenticationHandler.authenticate(context, "task_external_user", "127.0.0.1", new byte[0]);
            Assertions.assertEquals("task_successful_ldap", context.getSecurityIntegration());
            Assertions.assertEquals("task_external_user", context.getDistinguishedName());
        } finally {
            Config.authentication_chain = previousChain;
        }
    }

    @Test
    public void testLdapDNMappingGroup() throws Exception {
        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        authenticationMgr.createUser(new CreateUserStmt(
                new UserRef("ldap_user", "%", false, NodePosition.ZERO),
                true,
                new UserAuthOption("AUTHENTICATION_LDAP_SIMPLE",
                        "uid=ldap_user,ou=company,dc=example,dc=com",
                        false, NodePosition.ZERO),
                List.of(), Map.of(), NodePosition.ZERO));

        new MockUp<LDAPAuthProvider>() {
            @Mock
            private void checkPassword(String dn, String password) throws Exception {
                // mock: always success
            }

            @Mock
            private String findUserDNByRoot(String user, AccessControlContext ctx) throws Exception {
                return "uid=test,ou=People,dc=starrocks,dc=com";
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
        groups.put("ldap_user", Set.of("group1", "group2"));
        groups.put("uid=ldap_user,ou=company,dc=example,dc=com", Set.of("group3", "group4"));
        groups.put("u1", Set.of("group5"));
        groups.put("u2", Set.of("group6"));
        ldapGroupProvider.setUserToGroupCache(groups);

        ConnectContext context = new ConnectContext();
        AccessControlContext authCtx = context.getAccessControlContext();
        AuthenticationHandler.authenticate(context, "ldap_user", "%", "\0".getBytes(StandardCharsets.UTF_8));

        Assertions.assertEquals("ldap_user", authCtx.getQualifiedUser());
        Assertions.assertEquals("uid=ldap_user,ou=company,dc=example,dc=com", authCtx.getDistinguishedName());

        Assertions.assertEquals(Set.of("group1", "group2"),
                ldapGroupProvider.getGroup(authCtx.getCurrentUserIdentity(), authCtx.getDistinguishedName()));

        properties = new HashMap<>();
        properties.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "ldap");

        groupName = "ldap_group_provider2";
        authenticationMgr.replayCreateGroupProvider(groupName, properties);
        Config.group_provider = new String[] {groupName};
        LDAPGroupProvider ldapGroupProvider2 = (LDAPGroupProvider) authenticationMgr.getGroupProvider(groupName);

        Map<String, Set<String>> groups2 = new HashMap<>();
        groups2.put("ldap_user", Set.of("group1", "group2"));
        groups2.put("uid=ldap_user,ou=company,dc=example,dc=com", Set.of("group3", "group4"));
        groups2.put("u1", Set.of("group5"));
        groups2.put("u2", Set.of("group6"));
        ldapGroupProvider2.setUserToGroupCache(groups2);
        Assertions.assertEquals(Set.of("group3", "group4"),
                ldapGroupProvider2.getGroup(authCtx.getCurrentUserIdentity(), authCtx.getDistinguishedName()));
    }
}
