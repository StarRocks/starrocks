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

import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PEntryObject;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test cases for AuthorizationAnalyzer external user functionality
 */
public class AuthorizationAnalyzerExternalUserTest {
    static ConnectContext ctx;

    @BeforeAll
    public static void setUp() throws Exception {
        AnalyzeTestUtil.init();
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        UtFrameUtils.setUpForPersistTest();
        
        // Create a test role for granting privileges
        String createRoleSql = "test_role";
        AnalyzeTestUtil.getStarRocksAssert().withRole(createRoleSql);
    }

    @AfterAll
    public static void cleanup() throws Exception {
        // Clean up test role
        String dropRoleSql = "DROP ROLE test_role";
        StatementBase dropRoleStmt = UtFrameUtils.parseStmtWithNewParser(dropRoleSql, ctx);
        try {
            DDLStmtExecutor.execute(dropRoleStmt, ctx);
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }

    /**
     * Test that external user flag is correctly considered when creating UserIdentity
     */
    @Test
    public void testExternalUserIdentityCreation() throws Exception {
        String sql = "GRANT IMPERSONATE ON EXTERNAL USER 'alice' TO ROLE test_role";
        
        GrantPrivilegeStmt stmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        AuthorizationAnalyzer.analyze(stmt, ctx);
        
        // Check that the statement was analyzed successfully
        Assertions.assertEquals(ObjectType.USER, stmt.getObjectType());
        Assertions.assertNotNull(stmt.getObjectList());
        Assertions.assertEquals(1, stmt.getObjectList().size());
        
        // Check that the UserIdentity is created with external flag
        PEntryObject userObject = stmt.getObjectList().get(0);
        Assertions.assertNotNull(userObject);
        
        // Verify that the user object contains external user information
        // The actual implementation details depend on how UserPEntryObject stores external user info
        Assertions.assertTrue(userObject.toString().contains("alice"));
    }

    /**
     * Test that external user skips existence check during analysis
     */
    @Test
    public void testExternalUserSkipsExistenceCheck() throws Exception {
        // This should succeed even though 'non_existent_external_user' doesn't exist locally
        String sql = "GRANT IMPERSONATE ON EXTERNAL USER 'non_existent_external_user' TO ROLE test_role";
        
        GrantPrivilegeStmt stmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        
        // Should not throw exception for non-existent external user
        Assertions.assertDoesNotThrow(() -> {
            AuthorizationAnalyzer.analyze(stmt, ctx);
        });
        
        // Verify the statement was processed correctly
        Assertions.assertEquals(ObjectType.USER, stmt.getObjectType());
        Assertions.assertNotNull(stmt.getObjectList());
        Assertions.assertEquals(1, stmt.getObjectList().size());
    }

    /**
     * Test that regular (non-external) user still requires existence check
     */
    @Test
    public void testRegularUserRequiresExistenceCheck() throws Exception {
        // This should fail because 'non_existent_regular_user' doesn't exist locally
        String sql = "GRANT IMPERSONATE ON USER 'non_existent_regular_user' TO ROLE test_role";
        
        GrantPrivilegeStmt stmt = (GrantPrivilegeStmt) SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        
        // Should throw exception for non-existent regular user
        Assertions.assertThrows(SemanticException.class, () -> {
            AuthorizationAnalyzer.analyze(stmt, ctx);
        });
    }

    /**
     * Test REVOKE statement with external user
     */
    @Test
    public void testRevokePrivilegeOnExternalUser() throws Exception {
        String sql = "REVOKE IMPERSONATE ON EXTERNAL USER 'alice' FROM ROLE test_role";
        
        RevokePrivilegeStmt stmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        
        // Should not throw exception for external user in REVOKE
        Assertions.assertDoesNotThrow(() -> {
            AuthorizationAnalyzer.analyze(stmt, ctx);
        });
        
        // Verify the statement was processed correctly
        Assertions.assertEquals(ObjectType.USER, stmt.getObjectType());
        Assertions.assertNotNull(stmt.getObjectList());
        Assertions.assertEquals(1, stmt.getObjectList().size());
    }

    /**
     * Test multiple external users in single GRANT statement
     */
    @Test
    public void testMultipleExternalUsers() throws Exception {
        String sql = "GRANT IMPERSONATE ON EXTERNAL USER 'alice', 'bob', 'charlie' TO ROLE test_role";
        
        GrantPrivilegeStmt stmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        
        Assertions.assertDoesNotThrow(() -> {
            AuthorizationAnalyzer.analyze(stmt, ctx);
        });
        
        // Verify all external users were processed
        Assertions.assertEquals(ObjectType.USER, stmt.getObjectType());
        Assertions.assertNotNull(stmt.getObjectList());
        Assertions.assertEquals(3, stmt.getObjectList().size());
    }

    /**
     * Test mixed external and regular users (if supported by syntax)
     */
    @Test
    public void testExternalUserWithDifferentHostPatterns() throws Exception {
        // Test external user with specific host pattern
        String sql = "GRANT IMPERSONATE ON EXTERNAL USER 'alice'@'%.example.com' TO ROLE test_role";
        
        GrantPrivilegeStmt stmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        
        Assertions.assertDoesNotThrow(() -> {
            AuthorizationAnalyzer.analyze(stmt, ctx);
        });
        
        Assertions.assertEquals(ObjectType.USER, stmt.getObjectType());
        Assertions.assertNotNull(stmt.getObjectList());
        Assertions.assertEquals(1, stmt.getObjectList().size());
    }

    /**
     * Test that UserRef correctly identifies external users
     */
    @Test
    public void testUserRefExternalFlag() {
        // Test UserRef with external flag set to true
        UserRef externalUser = new UserRef("alice", "%", false, true, NodePosition.ZERO);
        Assertions.assertTrue(externalUser.isExternal());
        Assertions.assertEquals("alice", externalUser.getUser());
        Assertions.assertEquals("%", externalUser.getHost());
        
        // Test UserRef with external flag set to false
        UserRef regularUser = new UserRef("bob", "%", false, false, NodePosition.ZERO);
        Assertions.assertFalse(regularUser.isExternal());
        Assertions.assertEquals("bob", regularUser.getUser());
        Assertions.assertEquals("%", regularUser.getHost());
    }

    /**
     * Test UserIdentity creation with external flag
     */
    @Test
    public void testUserIdentityWithExternalFlag() {
        // Create UserIdentity with external (ephemeral) flag
        UserIdentity externalUserIdentity = new UserIdentity("alice", "%", false, true);
        Assertions.assertTrue(externalUserIdentity.isEphemeral());
        Assertions.assertEquals("alice", externalUserIdentity.getUser());
        Assertions.assertEquals("%", externalUserIdentity.getHost());
        
        // Create UserIdentity without external flag
        UserIdentity regularUserIdentity = new UserIdentity("bob", "%", false, false);
        Assertions.assertFalse(regularUserIdentity.isEphemeral());
        Assertions.assertEquals("bob", regularUserIdentity.getUser());
        Assertions.assertEquals("%", regularUserIdentity.getHost());
    }

    /**
     * Test EXECUTE AS EXTERNAL USER syntax parsing and analysis
     */
    @Test
    public void testExecuteAsExternalUserSyntax() throws Exception {
        // Test EXECUTE AS EXTERNAL USER syntax
        String sql = "EXECUTE AS EXTERNAL USER 'alice' WITH NO REVERT";
        
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assertions.assertInstanceOf(ExecuteAsStmt.class, stmt);
        
        ExecuteAsStmt executeAsStmt = (ExecuteAsStmt) stmt;
        Assertions.assertEquals("alice", executeAsStmt.getToUser().getUser());
        Assertions.assertTrue(executeAsStmt.getToUser().isExternal());
        Assertions.assertFalse(executeAsStmt.isAllowRevert());
        
        // Should not throw exception during analysis for external user
        Assertions.assertDoesNotThrow(() -> {
            AuthenticationAnalyzer.analyze(executeAsStmt, ctx);
        });
    }

    /**
     * Test EXECUTE AS regular USER syntax still works
     */
    @Test
    public void testExecuteAsRegularUserSyntax() throws Exception {
        // First create a regular user
        String createUserSql = "CREATE USER test_execute_user";
        StatementBase createUserStmt = UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
        DDLStmtExecutor.execute(createUserStmt, ctx);
        
        try {
            // Test EXECUTE AS USER syntax (without EXTERNAL)
            String sql = "EXECUTE AS USER 'test_execute_user' WITH NO REVERT";
            
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assertions.assertInstanceOf(ExecuteAsStmt.class, stmt);
            
            ExecuteAsStmt executeAsStmt = (ExecuteAsStmt) stmt;
            Assertions.assertEquals("test_execute_user", executeAsStmt.getToUser().getUser());
            Assertions.assertFalse(executeAsStmt.getToUser().isExternal());
            Assertions.assertFalse(executeAsStmt.isAllowRevert());
            
            // Should not throw exception during analysis for existing regular user
            Assertions.assertDoesNotThrow(() -> {
                AuthenticationAnalyzer.analyze(executeAsStmt, ctx);
            });
        } finally {
            // Clean up test user
            try {
                String dropUserSql = "DROP USER test_execute_user";
                StatementBase dropUserStmt = UtFrameUtils.parseStmtWithNewParser(dropUserSql, ctx);
                DDLStmtExecutor.execute(dropUserStmt, ctx);
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }
}
