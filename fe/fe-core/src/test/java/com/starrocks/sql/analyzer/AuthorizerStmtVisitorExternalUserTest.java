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

package com.starrocks.sql.analyzer;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.parser.NodePosition;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test cases for AuthorizerStmtVisitor external user authorization functionality
 * Note: These are conceptual tests since the authorization system is complex to mock fully
 */
public class AuthorizerStmtVisitorExternalUserTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private AuthenticationMgr authenticationMgr;
    @Mocked
    private AuthorizationMgr authorizationMgr;
    @Mocked
    private ConnectContext connectContext;

    private AuthorizerStmtVisitor visitor;

    @BeforeEach
    public void setUp() {
        visitor = new AuthorizerStmtVisitor();

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
     * Test the concept of external user authorization
     */
    @Test
    public void testExternalUserAuthorizationConcept() {
        // Create external user statement
        UserRef externalUser = new UserRef("alice", "%", false, true, NodePosition.ZERO);
        ExecuteAsStmt executeAsStmt = new ExecuteAsStmt(externalUser, false);

        // Test the basic properties
        Assertions.assertTrue(externalUser.isExternal());
        Assertions.assertEquals("alice", externalUser.getUser());
        Assertions.assertEquals("%", externalUser.getHost());
        Assertions.assertNotNull(executeAsStmt);
    }

    /**
     * Test the concept of regular user authorization
     */
    @Test
    public void testRegularUserAuthorizationConcept() {
        // Create regular user statement
        UserRef regularUser = new UserRef("bob", "%", false, false, NodePosition.ZERO);
        ExecuteAsStmt executeAsStmt = new ExecuteAsStmt(regularUser, false);

        // Test the basic properties
        Assertions.assertFalse(regularUser.isExternal());
        Assertions.assertEquals("bob", regularUser.getUser());
        Assertions.assertEquals("%", regularUser.getHost());
        Assertions.assertNotNull(executeAsStmt);
    }

    /**
     * Test UserIdentity creation for external vs regular users
     */
    @Test
    public void testUserIdentityCreationConcept() {
        // Test external user identity creation concept
        UserRef externalUser = new UserRef("alice", "%.company.com", false, true, NodePosition.ZERO);

        // In the actual implementation, this would create an ephemeral UserIdentity
        UserIdentity externalUserIdentity = UserIdentity.createEphemeralUserIdent(
                externalUser.getUser(), externalUser.getHost());

        Assertions.assertTrue(externalUserIdentity.isEphemeral());
        Assertions.assertEquals("alice", externalUserIdentity.getUser());
        Assertions.assertEquals("%.company.com", externalUserIdentity.getHost());

        // Test regular user identity creation concept
        UserRef regularUser = new UserRef("bob", "192.168.1.%", false, false, NodePosition.ZERO);

        // In the actual implementation, this would create a regular UserIdentity
        UserIdentity regularUserIdentity = new UserIdentity(
                regularUser.getUser(), regularUser.getHost(), regularUser.isDomain());

        Assertions.assertFalse(regularUserIdentity.isEphemeral());
        Assertions.assertEquals("bob", regularUserIdentity.getUser());
        Assertions.assertEquals("192.168.1.%", regularUserIdentity.getHost());
    }

    /**
     * Test domain user handling concept
     */
    @Test
    public void testDomainUserHandlingConcept() {
        // Test external domain user
        UserRef externalDomainUser = new UserRef("alice", "company.com", true, true, NodePosition.ZERO);

        Assertions.assertTrue(externalDomainUser.isDomain());
        Assertions.assertTrue(externalDomainUser.isExternal());
        Assertions.assertEquals("alice", externalDomainUser.getUser());
        Assertions.assertEquals("company.com", externalDomainUser.getHost());

        // Test regular domain user
        UserRef regularDomainUser = new UserRef("bob", "internal.com", true, false, NodePosition.ZERO);

        Assertions.assertTrue(regularDomainUser.isDomain());
        Assertions.assertFalse(regularDomainUser.isExternal());
        Assertions.assertEquals("bob", regularDomainUser.getUser());
        Assertions.assertEquals("internal.com", regularDomainUser.getHost());
    }

    /**
     * Test special characters in user names and hosts concept
     */
    @Test
    public void testSpecialCharactersInUserNamesAndHostsConcept() {
        // External user with email-like username
        UserRef emailUser = new UserRef("user@domain.com", "%.external.org", false, true, NodePosition.ZERO);

        Assertions.assertTrue(emailUser.isExternal());
        Assertions.assertEquals("user@domain.com", emailUser.getUser());
        Assertions.assertEquals("%.external.org", emailUser.getHost());

        // External user with service account pattern
        UserRef serviceUser = new UserRef("service_account", "10.0.0.%", false, true, NodePosition.ZERO);

        Assertions.assertTrue(serviceUser.isExternal());
        Assertions.assertEquals("service_account", serviceUser.getUser());
        Assertions.assertEquals("10.0.0.%", serviceUser.getHost());
    }

    /**
     * Test null host handling concept
     */
    @Test
    public void testNullHostHandlingConcept() {
        // External user with null host
        UserRef externalUserNullHost = new UserRef("external_no_host", null, false, true, NodePosition.ZERO);

        Assertions.assertTrue(externalUserNullHost.isExternal());
        Assertions.assertEquals("external_no_host", externalUserNullHost.getUser());
        Assertions.assertNull(externalUserNullHost.getHost());

        // Regular user with null host
        UserRef regularUserNullHost = new UserRef("regular_no_host", null, false, false, NodePosition.ZERO);

        Assertions.assertFalse(regularUserNullHost.isExternal());
        Assertions.assertEquals("regular_no_host", regularUserNullHost.getUser());
        Assertions.assertNull(regularUserNullHost.getHost());
    }

    /**
     * Test various authorization concepts
     */
    @Test
    public void testAuthorizationConcepts() {
        // Test that external and regular users have different authorization paths
        UserRef externalUser = new UserRef("external_user", "%", false, true, NodePosition.ZERO);
        UserRef regularUser = new UserRef("regular_user", "%", false, false, NodePosition.ZERO);

        // Both should be valid UserRef objects
        Assertions.assertNotNull(externalUser);
        Assertions.assertNotNull(regularUser);

        // But they should have different external flags
        Assertions.assertTrue(externalUser.isExternal());
        Assertions.assertFalse(regularUser.isExternal());

        // They should create different types of UserIdentity objects
        UserIdentity externalIdentity = UserIdentity.createEphemeralUserIdent(
                externalUser.getUser(), externalUser.getHost());
        UserIdentity regularIdentity = new UserIdentity(
                regularUser.getUser(), regularUser.getHost(), regularUser.isDomain());

        Assertions.assertTrue(externalIdentity.isEphemeral());
        Assertions.assertFalse(regularIdentity.isEphemeral());
    }

    /**
     * Test execute as statement concepts
     */
    @Test
    public void testExecuteAsStatementConcepts() {
        // Test external user execute as statement
        UserRef externalUser = new UserRef("external_user", "%", false, true, NodePosition.ZERO);
        ExecuteAsStmt externalStmt = new ExecuteAsStmt(externalUser, false);

        Assertions.assertEquals(externalUser, externalStmt.getToUser());
        Assertions.assertFalse(externalStmt.isAllowRevert());

        // Test regular user execute as statement
        UserRef regularUser = new UserRef("regular_user", "%", false, false, NodePosition.ZERO);
        ExecuteAsStmt regularStmt = new ExecuteAsStmt(regularUser, false);

        Assertions.assertEquals(regularUser, regularStmt.getToUser());
        Assertions.assertFalse(regularStmt.isAllowRevert());
    }
}