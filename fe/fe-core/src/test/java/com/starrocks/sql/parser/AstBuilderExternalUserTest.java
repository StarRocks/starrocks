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

package com.starrocks.sql.parser;

import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserRef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Comprehensive test cases for external user functionality
 * Tests both AST generation using SqlParser.parse and UserRef class functionality
 */
public class AstBuilderExternalUserTest {

    /**
     * Test EXECUTE AS EXTERNAL USER statement parsing
     */
    @Test
    public void testExecuteAsExternalUser() {
        String sql = "EXECUTE AS EXTERNAL USER 'alice'@'%' WITH NO REVERT";

        List<StatementBase> parseNodes = SqlParser.parse(sql, new SessionVariable());
        Assertions.assertNotNull(parseNodes);
        Assertions.assertEquals(1, parseNodes.size());

        ExecuteAsStmt stmt = (ExecuteAsStmt) parseNodes.get(0);
        Assertions.assertNotNull(stmt);

        UserRef userRef = stmt.getToUser();
        Assertions.assertEquals("alice", userRef.getUser());
        Assertions.assertEquals("%", userRef.getHost());
        Assertions.assertTrue(userRef.isExternal());
        Assertions.assertFalse(userRef.isDomain());
        Assertions.assertFalse(stmt.isAllowRevert());
    }

    /**
     * Test GRANT ON EXTERNAL USER statement parsing
     */
    @Test
    public void testGrantOnExternalUser() {
        String sql = "GRANT IMPERSONATE ON EXTERNAL USER 'alice'@'%' TO ROLE test_role";

        List<StatementBase> parseNodes = SqlParser.parse(sql, new SessionVariable());
        Assertions.assertNotNull(parseNodes);
        Assertions.assertEquals(1, parseNodes.size());

        GrantPrivilegeStmt stmt = (GrantPrivilegeStmt) parseNodes.get(0);
        Assertions.assertNotNull(stmt);
        Assertions.assertTrue(stmt.getUserPrivilegeObjectList().get(0).isExternal());
    }

    /**
     * Test REVOKE ON EXTERNAL USER statement parsing
     */
    @Test
    public void testRevokeOnExternalUser() {
        String sql = "REVOKE IMPERSONATE ON EXTERNAL USER 'alice'@'%' FROM ROLE test_role";

        List<StatementBase> parseNodes = SqlParser.parse(sql, new SessionVariable());
        Assertions.assertNotNull(parseNodes);
        Assertions.assertEquals(1, parseNodes.size());

        RevokePrivilegeStmt stmt = (RevokePrivilegeStmt) parseNodes.get(0);
        Assertions.assertNotNull(stmt);

        Assertions.assertTrue(stmt.getUserPrivilegeObjectList().get(0).isExternal());
    }

    /**
     * Test EXECUTE AS EXTERNAL USER with different host patterns
     */
    @Test
    public void testExecuteAsExternalUserWithDifferentHosts() {
        String[] sqlStatements = {
                "EXECUTE AS EXTERNAL USER 'alice'@'%' WITH NO REVERT",
                "EXECUTE AS EXTERNAL USER 'bob'@'%.company.com' WITH NO REVERT",
                "EXECUTE AS EXTERNAL USER 'charlie'@'192.168.1.%' WITH NO REVERT"
        };

        for (String sql : sqlStatements) {
            List<StatementBase> parseNodes = SqlParser.parse(sql, new SessionVariable());
            Assertions.assertNotNull(parseNodes);
            Assertions.assertEquals(1, parseNodes.size());

            ExecuteAsStmt stmt = (ExecuteAsStmt) parseNodes.get(0);
            UserRef userRef = stmt.getToUser();
            Assertions.assertTrue(userRef.isExternal());
            Assertions.assertFalse(stmt.isAllowRevert());
        }
    }

    /**
     * Test EXECUTE AS EXTERNAL USER with domain
     */
    @Test
    public void testExecuteAsExternalUserWithDomain() {
        String sql = "EXECUTE AS EXTERNAL USER 'alice'@['company.com'] WITH NO REVERT";

        List<StatementBase> parseNodes = SqlParser.parse(sql, new SessionVariable());
        Assertions.assertNotNull(parseNodes);
        Assertions.assertEquals(1, parseNodes.size());

        ExecuteAsStmt stmt = (ExecuteAsStmt) parseNodes.get(0);
        UserRef userRef = stmt.getToUser();
        Assertions.assertEquals("alice", userRef.getUser());
        Assertions.assertEquals("company.com", userRef.getHost());
        Assertions.assertTrue(userRef.isExternal());
        Assertions.assertTrue(userRef.isDomain());
        Assertions.assertFalse(stmt.isAllowRevert());
    }

    // ========== Additional UserRef functionality tests ==========

    /**
     * Test UserRef equality and hashCode with external flag
     */
    @Test
    public void testUserRefEqualityWithExternalFlag() {
        UserRef user1 = new UserRef("alice", "%", false, false, NodePosition.ZERO);
        UserRef user2 = new UserRef("alice", "%", false, false, NodePosition.ZERO);
        UserRef externalUser1 = new UserRef("alice", "%", false, true, NodePosition.ZERO);
        UserRef externalUser2 = new UserRef("alice", "%", false, true, NodePosition.ZERO);

        // Same regular users should be equal
        Assertions.assertEquals(user1, user2);
        Assertions.assertEquals(user1.hashCode(), user2.hashCode());

        // Same external users should be equal
        Assertions.assertEquals(externalUser1, externalUser2);
        Assertions.assertEquals(externalUser1.hashCode(), externalUser2.hashCode());

        // Regular and external users with same name/host should NOT be equal
        Assertions.assertNotEquals(user1, externalUser1);
        Assertions.assertNotEquals(user1.hashCode(), externalUser1.hashCode());
    }

    /**
     * Test UserRef constructor with all parameters
     */
    @Test
    public void testUserRefConstructorWithAllParameters() {
        UserRef userRef = new UserRef("test_user", "test_host", true, true, NodePosition.ZERO);

        Assertions.assertEquals("test_user", userRef.getUser());
        Assertions.assertEquals("test_host", userRef.getHost());
        Assertions.assertTrue(userRef.isDomain());
        Assertions.assertTrue(userRef.isExternal());
        Assertions.assertEquals(NodePosition.ZERO, userRef.getPos());
    }

    /**
     * Test UserRef constructor with legacy parameters (without external flag)
     */
    @Test
    public void testUserRefConstructorLegacy() {
        UserRef userRef = new UserRef("legacy_user", "legacy_host", false, NodePosition.ZERO);

        Assertions.assertEquals("legacy_user", userRef.getUser());
        Assertions.assertEquals("legacy_host", userRef.getHost());
        Assertions.assertFalse(userRef.isDomain());
        Assertions.assertFalse(userRef.isExternal()); // Should default to false
        Assertions.assertEquals(NodePosition.ZERO, userRef.getPos());
    }

    /**
     * Test UserRef constructor with basic parameters (default constructor behavior)
     */
    @Test
    public void testUserRefConstructorBasic() {
        UserRef userRef = new UserRef("basic_user", "basic_host");

        Assertions.assertEquals("basic_user", userRef.getUser());
        Assertions.assertEquals("basic_host", userRef.getHost());
        Assertions.assertFalse(userRef.isDomain()); // Should default to false
        Assertions.assertFalse(userRef.isExternal()); // Should default to false
    }

    /**
     * Test toString method with external users
     */
    @Test
    public void testToStringWithExternalUsers() {
        // Regular user
        UserRef regularUser = new UserRef("alice", "%", false, false, NodePosition.ZERO);
        String regularString = regularUser.toString();
        Assertions.assertTrue(regularString.contains("alice"));
        Assertions.assertTrue(regularString.contains("%"));

        // External user
        UserRef externalUser = new UserRef("bob", "%.example.com", false, true, NodePosition.ZERO);
        String externalString = externalUser.toString();
        Assertions.assertTrue(externalString.contains("bob"));
        Assertions.assertTrue(externalString.contains("%.example.com"));

        // Domain user
        UserRef domainUser = new UserRef("charlie", "company.com", true, false, NodePosition.ZERO);
        String domainString = domainUser.toString();
        Assertions.assertTrue(domainString.contains("charlie"));
        Assertions.assertTrue(domainString.contains("company.com"));

        // External domain user
        UserRef externalDomainUser = new UserRef("david", "external.org", true, true, NodePosition.ZERO);
        String externalDomainString = externalDomainUser.toString();
        Assertions.assertTrue(externalDomainString.contains("david"));
        Assertions.assertTrue(externalDomainString.contains("external.org"));
    }

    /**
     * Test null host handling
     */
    @Test
    public void testNullHostHandling() {
        UserRef userWithNullHost = new UserRef("user_no_host", null, false, false, NodePosition.ZERO);

        Assertions.assertEquals("user_no_host", userWithNullHost.getUser());
        Assertions.assertNull(userWithNullHost.getHost());
        Assertions.assertFalse(userWithNullHost.isDomain());
        Assertions.assertFalse(userWithNullHost.isExternal());

        // External user with null host
        UserRef externalUserWithNullHost = new UserRef("external_no_host", null, false, true, NodePosition.ZERO);
        Assertions.assertEquals("external_no_host", externalUserWithNullHost.getUser());
        Assertions.assertNull(externalUserWithNullHost.getHost());
        Assertions.assertFalse(externalUserWithNullHost.isDomain());
        Assertions.assertTrue(externalUserWithNullHost.isExternal());
    }

    /**
     * Test empty string host handling
     */
    @Test
    public void testEmptyStringHostHandling() {
        UserRef userWithEmptyHost = new UserRef("user_empty_host", "", false, true, NodePosition.ZERO);

        Assertions.assertEquals("user_empty_host", userWithEmptyHost.getUser());
        // Empty string should be converted to null according to Strings.emptyToNull
        Assertions.assertNull(userWithEmptyHost.getHost());
        Assertions.assertFalse(userWithEmptyHost.isDomain());
        Assertions.assertTrue(userWithEmptyHost.isExternal());
    }

    /**
     * Test user names with special characters for external users
     */
    @Test
    public void testUserNamesWithSpecialCharactersForExternalUsers() {
        // User name with @ symbol
        UserRef emailUser = new UserRef("user@domain.com", "%", false, true, NodePosition.ZERO);
        Assertions.assertEquals("user@domain.com", emailUser.getUser());
        Assertions.assertTrue(emailUser.isExternal());

        // User name with underscore
        UserRef underscoreUser = new UserRef("service_user", "%", false, true, NodePosition.ZERO);
        Assertions.assertEquals("service_user", underscoreUser.getUser());
        Assertions.assertTrue(underscoreUser.isExternal());

        // User name with hyphen
        UserRef hyphenUser = new UserRef("test-user", "%", false, true, NodePosition.ZERO);
        Assertions.assertEquals("test-user", hyphenUser.getUser());
        Assertions.assertTrue(hyphenUser.isExternal());

        // User name with numbers
        UserRef numericUser = new UserRef("user123", "%", false, true, NodePosition.ZERO);
        Assertions.assertEquals("user123", numericUser.getUser());
        Assertions.assertTrue(numericUser.isExternal());
    }

    /**
     * Test NodePosition handling
     */
    @Test
    public void testNodePositionHandling() {
        NodePosition customPosition = new NodePosition(10, 5);
        UserRef userWithPosition = new UserRef("positioned_user", "%", false, true, customPosition);

        Assertions.assertEquals("positioned_user", userWithPosition.getUser());
        Assertions.assertTrue(userWithPosition.isExternal());
        Assertions.assertEquals(customPosition, userWithPosition.getPos());
    }

    /**
     * Test backward compatibility with existing constructors
     */
    @Test
    public void testBackwardCompatibility() {
        // Test that existing code using old constructors still works
        UserRef oldStyleUser1 = new UserRef("old_user1", "old_host1");
        Assertions.assertEquals("old_user1", oldStyleUser1.getUser());
        Assertions.assertEquals("old_host1", oldStyleUser1.getHost());
        Assertions.assertFalse(oldStyleUser1.isExternal()); // Should default to false

        UserRef oldStyleUser2 = new UserRef("old_user2", "old_host2", true, NodePosition.ZERO);
        Assertions.assertEquals("old_user2", oldStyleUser2.getUser());
        Assertions.assertEquals("old_host2", oldStyleUser2.getHost());
        Assertions.assertTrue(oldStyleUser2.isDomain());
        Assertions.assertFalse(oldStyleUser2.isExternal()); // Should default to false
    }
}