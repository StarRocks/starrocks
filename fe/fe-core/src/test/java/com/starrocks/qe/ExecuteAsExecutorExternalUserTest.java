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

package com.starrocks.qe;

import com.google.common.collect.Sets;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.AuthenticationProvider;
import com.starrocks.authentication.GroupProvider;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.authentication.UserProperty;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.parser.NodePosition;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test cases for ExecuteAsExecutor user functionality
 * Tests both external and native user behavior with group/role refresh and security integration
 */
public class ExecuteAsExecutorExternalUserTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private AuthenticationMgr authenticationMgr;
    @Mocked
    private AuthorizationMgr authorizationMgr;

    @BeforeEach
    public void setUp() {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getAuthenticationMgr();
                minTimes = 0;
                result = authenticationMgr;

                globalStateMgr.getAuthorizationMgr();
                minTimes = 0;
                result = authorizationMgr;
            }
        };
    }

    /**
     * Core test case demonstrating ExecuteAs with external user - should refresh groups and roles
     */
    @Test
    public void testExecuteAsExternalUserRefreshesGroupsAndRoles() throws Exception {
        // Mock external groups and roles
        Set<String> mockGroups = Sets.newHashSet("starrocks_admin", "starrocks_user");

        GroupProvider mockedGroupProvider = new GroupProvider("test_group_provider", Map.of()) {
            @Override
            public Set<String> getGroup(UserIdentity userIdentity, String distinguishedName) {
                Assertions.assertEquals("alice", userIdentity.getUser());
                Assertions.assertTrue(userIdentity.isEphemeral());
                return mockGroups;
            }

            @Override
            public void checkProperty() throws SemanticException {
                // No validation needed for test
            }
        };

        SecurityIntegration mockedIntegration = new SecurityIntegration("test_integration", Map.of()) {
            @Override
            public AuthenticationProvider getAuthenticationProvider() throws AuthenticationException {
                return null;
            }

            @Override
            public List<String> getGroupProviderName() {
                return List.of("test_group_provider");
            }
        };

        new MockUp<AuthenticationMgr>() {
            @Mock
            public GroupProvider getGroupProvider(String name) {
                if (name.equals("test_group_provider")) {
                    return mockedGroupProvider;
                }
                return null;
            }

            @Mock
            public SecurityIntegration getSecurityIntegration(String name) {
                if (name.equals("test_integration")) {
                    return mockedIntegration;
                }
                return null;
            }
        };

        new MockUp<AuthorizationMgr>() {
            @Mock
            public Set<Long> getRoleIdListByGroup(String groupName) {
                if (groupName.equals("starrocks_admin")) {
                    return Set.of(1L, 2L);
                } else if (groupName.equals("starrocks_user")) {
                    return Set.of(3L);
                }
                return Collections.emptySet();
            }
        };

        ConnectContext context = new ConnectContext();
        context.setSecurityIntegration("test_integration");

        // Create external user statement
        UserRef externalUser = new UserRef("alice", "%", false, true, NodePosition.ZERO);
        ExecuteAsStmt stmt = new ExecuteAsStmt(externalUser, false);

        // Execute the statement
        ExecuteAsExecutor.execute(stmt, context);

        // Verify that groups are actually set in the ConnectContext
        Set<String> actualGroups = context.getGroups();
        Assertions.assertEquals(mockGroups, actualGroups,
                "Groups should be correctly set in ConnectContext for external user");
        Assertions.assertFalse(actualGroups.isEmpty(),
                "Groups should not be empty for external user");

        // Verify roles are set correctly
        Set<Long> expectedRoles = Set.of(1L, 2L, 3L);
        Assertions.assertEquals(expectedRoles, context.getCurrentRoleIds(),
                "Roles should be correctly set in ConnectContext for external user");
    }

    /**
     * Test handling of exception during group refresh
     */
    @Test
    public void testGroupRefreshExceptionHandling() throws Exception {
        GroupProvider mockedGroupProvider = new GroupProvider("exception_group_provider", Map.of()) {
            @Override
            public Set<String> getGroup(UserIdentity userIdentity, String distinguishedName) {
                throw new RuntimeException("Failed to get groups");
            }

            @Override
            public void checkProperty() throws SemanticException {
                // No validation needed for test
            }
        };

        SecurityIntegration mockedIntegration = new SecurityIntegration("exception_integration", Map.of()) {
            @Override
            public AuthenticationProvider getAuthenticationProvider() throws AuthenticationException {
                return null;
            }

            @Override
            public List<String> getGroupProviderName() {
                return List.of("exception_group_provider");
            }
        };

        new MockUp<AuthenticationMgr>() {
            @Mock
            public GroupProvider getGroupProvider(String name) {
                if (name.equals("exception_group_provider")) {
                    return mockedGroupProvider;
                }
                return null;
            }

            @Mock
            public SecurityIntegration getSecurityIntegration(String name) {
                if (name.equals("exception_integration")) {
                    return mockedIntegration;
                }
                return null;
            }
        };

        new MockUp<AuthorizationMgr>() {
            @Mock
            public Set<Long> getRoleIdListByGroup(String groupName) {
                return Collections.emptySet();
            }
        };

        ConnectContext context = new ConnectContext();
        context.setSecurityIntegration("exception_integration");

        // Create external user statement
        UserRef externalUser = new UserRef("alice", "%", false, true, NodePosition.ZERO);
        ExecuteAsStmt stmt = new ExecuteAsStmt(externalUser, false);

        // Should not throw exception even if group refresh fails
        Assertions.assertDoesNotThrow(() -> {
            ExecuteAsExecutor.execute(stmt, context);
        });

        // Verify that groups are NOT set when exception occurs during group refresh
        Set<String> actualGroups = context.getGroups();
        Assertions.assertTrue(actualGroups.isEmpty(),
                "Groups should be empty when group refresh fails due to exception");

        // Verify that roles are also empty when group refresh fails
        Set<Long> actualRoles = context.getCurrentRoleIds();
        Assertions.assertTrue(actualRoles.isEmpty(),
                "Roles should be empty when group refresh fails due to exception");
    }

    /**
     * Test ExecuteAs with native user - should also refresh groups and roles
     */
    @Test
    public void testExecuteAsNativeUserRefreshesGroupsAndRoles() throws Exception {
        // Mock groups for native user
        Set<String> mockGroups = Sets.newHashSet("native_group1", "native_group2");

        GroupProvider mockedGroupProvider = new GroupProvider("native_group_provider", Map.of()) {
            @Override
            public Set<String> getGroup(UserIdentity userIdentity, String distinguishedName) {
                Assertions.assertEquals("user1", userIdentity.getUser());
                Assertions.assertFalse(userIdentity.isEphemeral()); // Native user should not be ephemeral
                return mockGroups;
            }

            @Override
            public void checkProperty() throws SemanticException {
                // No validation needed for test
            }
        };

        SecurityIntegration mockedIntegration = new SecurityIntegration("native_integration", Map.of()) {
            @Override
            public AuthenticationProvider getAuthenticationProvider() throws AuthenticationException {
                return null;
            }

            @Override
            public List<String> getGroupProviderName() {
                return List.of("native_group_provider");
            }
        };

        new MockUp<AuthenticationMgr>() {
            @Mock
            public GroupProvider getGroupProvider(String name) {
                if (name.equals("native_group_provider")) {
                    return mockedGroupProvider;
                }
                return null;
            }

            @Mock
            public SecurityIntegration getSecurityIntegration(String name) {
                if (name.equals("native_integration")) {
                    return mockedIntegration;
                }
                return null;
            }

            @Mock
            public UserProperty getUserProperty(String userName) {
                return new UserProperty();
            }
        };

        new MockUp<AuthorizationMgr>() {
            @Mock
            public Set<Long> getRoleIdListByGroup(String groupName) {
                if (groupName.equals("native_group1")) {
                    return Set.of(10L, 20L);
                } else if (groupName.equals("native_group2")) {
                    return Set.of(30L);
                }
                return Set.of();
            }

            @Mock
            public Set<Long> getDefaultRoleIdsByUser(UserIdentity user) throws PrivilegeException {
                return new HashSet<>(Set.of(1L));
            }
        };

        ConnectContext context = new ConnectContext();
        context.setSessionVariable(new SessionVariable());
        context.setSecurityIntegration("native_integration");

        // Create native user statement
        UserRef nativeUser = new UserRef("user1", "%", false, false, NodePosition.ZERO);
        ExecuteAsStmt stmt = new ExecuteAsStmt(nativeUser, false);

        // Execute the statement
        ExecuteAsExecutor.execute(stmt, context);

        // Verify that groups are actually set in the ConnectContext
        Set<String> actualGroups = context.getGroups();
        Assertions.assertEquals(mockGroups, actualGroups,
                "Groups should be correctly set in ConnectContext for native user");
        Assertions.assertFalse(actualGroups.isEmpty(),
                "Groups should not be empty for native user");

        Set<Long> expectedRoles = Set.of(1L, 10L, 20L, 30L);
        Assertions.assertEquals(expectedRoles, context.getCurrentRoleIds(),
                "Roles should be correctly set in ConnectContext for native user");
    }

    /**
     * Test UserIdentity creation for external users
     */
    @Test
    public void testUserIdentityCreation() throws Exception {
        GroupProvider externalGroupProvider = new GroupProvider("external_group_provider", Map.of()) {
            @Override
            public Set<String> getGroup(UserIdentity userIdentity, String distinguishedName) {
                // Verify that UserIdentity is created as ephemeral for external user
                Assertions.assertTrue(userIdentity.isEphemeral());
                Assertions.assertEquals("alice", userIdentity.getUser());
                return Sets.newHashSet("external_group1", "external_group2");
            }

            @Override
            public void checkProperty() throws SemanticException {
                // No validation needed for test
            }
        };

        SecurityIntegration externalIntegration = new SecurityIntegration("external_integration", Map.of()) {
            @Override
            public AuthenticationProvider getAuthenticationProvider() throws AuthenticationException {
                return null;
            }

            @Override
            public List<String> getGroupProviderName() {
                return List.of("external_group_provider");
            }
        };

        new MockUp<AuthenticationMgr>() {
            @Mock
            public GroupProvider getGroupProvider(String name) {
                if (name.equals("external_group_provider")) {
                    return externalGroupProvider;
                }
                return null;
            }

            @Mock
            public SecurityIntegration getSecurityIntegration(String name) {
                if (name.equals("external_integration")) {
                    return externalIntegration;
                }
                return null;
            }
        };

        new MockUp<AuthorizationMgr>() {
            @Mock
            public Set<Long> getRoleIdListByGroup(String groupName) {
                if (groupName.equals("external_group1")) {
                    return Set.of(1L);
                } else if (groupName.equals("external_group2")) {
                    return Set.of(2L);
                }
                return Collections.emptySet();
            }
        };

        ConnectContext externalContext = new ConnectContext();
        externalContext.setSecurityIntegration("external_integration");

        // Test external user
        UserRef externalUser = new UserRef("alice", "%", false, true, NodePosition.ZERO);
        ExecuteAsStmt externalStmt = new ExecuteAsStmt(externalUser, false);
        ExecuteAsExecutor.execute(externalStmt, externalContext);

        Assertions.assertEquals(new UserIdentity("alice", "%"), externalContext.getCurrentUserIdentity());

        // Verify that groups are set for external user
        Set<String> externalGroups = externalContext.getGroups();
        Assertions.assertFalse(externalGroups.isEmpty(),
                "Groups should be set for external user");
        Assertions.assertEquals(Set.of("external_group1", "external_group2"), externalGroups);

        // Verify roles are set for external user
        Set<Long> externalRoles = externalContext.getCurrentRoleIds();
        Assertions.assertEquals(Set.of(1L, 2L), externalRoles);
    }

    /**
     * Test full flow of ExecuteAs with external user including group and role refresh
     */
    @Test
    public void testNormal() {
        GroupProvider mockedGroupProvider = new GroupProvider("mocked_group_provider", Map.of()) {
            @Override
            public Set<String> getGroup(UserIdentity userIdentity, String distinguishedName) {
                return Set.of("g1", "g2");
            }

            @Override
            public void checkProperty() throws SemanticException {

            }
        };

        SecurityIntegration mockedIntegration = new SecurityIntegration("mocked_integration", Map.of()) {
            @Override
            public AuthenticationProvider getAuthenticationProvider() throws AuthenticationException {
                return null;
            }

            @Override
            public List<String> getGroupProviderName() {
                return List.of("mocked_group_provider");
            }
        };

        new MockUp<AuthenticationMgr>() {
            @Mock
            public GroupProvider getGroupProvider(String name) {
                return mockedGroupProvider;
            }

            @Mock
            public SecurityIntegration getSecurityIntegration(String name) {
                return mockedIntegration;
            }
        };

        new MockUp<AuthorizationMgr>() {
            @Mock
            public Set<Long> getRoleIdListByGroup(String groupName) {
                if (groupName.equals("g1")) {
                    return Set.of(1L, 2L);
                } else if (groupName.equals("g2")) {
                    return Set.of(3L);
                }
                return Collections.emptySet();
            }
        };

        ConnectContext context = new ConnectContext();
        context.setSecurityIntegration("mocked_integration");

        UserRef externalUser = new UserRef("alice", "%", false, true, NodePosition.ZERO);
        ExecuteAsStmt stmt = new ExecuteAsStmt(externalUser, false);

        // Execute the statement
        ExecuteAsExecutor.execute(stmt, context);

        Set<String> mockGroups = Sets.newHashSet("g1", "g2");
        Assertions.assertEquals(mockGroups, context.getGroups(),
                "Groups should be correctly set in ConnectContext for external user");
        Assertions.assertEquals(Set.of(1L, 2L, 3L), context.getCurrentRoleIds(),
                "Roles should be correctly set in ConnectContext for external user");
    }
}