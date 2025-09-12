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


package com.starrocks.sql.ast;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.UserProperty;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ExecuteAsExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.AstBuilder;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExecuteAsStmtTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private AuthenticationMgr auth;
    @Mocked
    private AuthorizationMgr authorizationMgr;

    @BeforeEach
    public void setUp() throws PrivilegeException {

        SqlParser sqlParser = new SqlParser(AstBuilder.getInstance());
        Analyzer analyzer = new Analyzer(Analyzer.AnalyzerVisitor.getInstance());
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getAuthenticationMgr();
                minTimes = 0;
                result = auth;

                GlobalStateMgr.getCurrentState().getAuthorizationMgr();
                minTimes = 0;
                result = authorizationMgr;

                authorizationMgr.getDefaultRoleIdsByUser((UserIdentity) any);
                minTimes = 0;
                result = new HashSet<>();
            }
        };

        new Expectations() {
            {
                globalStateMgr.getSqlParser();
                minTimes = 0;
                result = sqlParser;

                globalStateMgr.getAnalyzer();
                minTimes = 0;
                result = analyzer;
            }
        };
    }

    @Test
    public void testWithNoRevert() throws Exception {
        // suppose current user exists
        new Expectations(auth) {
            {
                auth.doesUserExist((UserIdentity) any);
                minTimes = 0;
                result = true;

                auth.getUserProperty(anyString);
                minTimes = 0;
                result = new UserProperty();
            }
        };

        ConnectContext connectContext = new ConnectContext();
        ExecuteAsStmt stmt = (ExecuteAsStmt) com.starrocks.sql.parser.SqlParser.parse(
                "execute as user1 with no revert", 1).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        Assertions.assertEquals("user1", stmt.getToUser().getUser());
        Assertions.assertEquals("%", stmt.getToUser().getHost());
        Assertions.assertEquals("EXECUTE AS 'user1'@'%' WITH NO REVERT", stmt.toString());
        Assertions.assertFalse(stmt.isAllowRevert());

        ExecuteAsExecutor.execute(stmt, connectContext);

        Assertions.assertEquals(new UserIdentity("user1", "%"), connectContext.getCurrentUserIdentity());
    }

    @Test
    public void testUserNotExist() {
        ConnectContext ctx = new ConnectContext();
        assertThrows(SemanticException.class, () -> {
            // suppose current user doesn't exist, check for exception
            new Expectations(auth) {
                {
                    auth.doesUserExist((UserIdentity) any);
                    minTimes = 0;
                    result = false;
                }
            };
            ExecuteAsStmt stmt = (ExecuteAsStmt) com.starrocks.sql.parser.SqlParser.parse(
                    "execute as user1", 1).get(0);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testAllowRevert() {
        ConnectContext ctx = new ConnectContext();
        assertThrows(SemanticException.class, () -> {
            // suppose current user exists
            new Expectations(auth) {
                {
                    auth.doesUserExist((UserIdentity) any);
                    minTimes = 0;
                    result = true;
                }
            };

            ExecuteAsStmt stmt = (ExecuteAsStmt) com.starrocks.sql.parser.SqlParser.parse(
                    "execute as user1", 1).get(0);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testExternalUserImpersonate() throws Exception {
        ConnectContext ctx = new ConnectContext();
        // Test EXECUTE AS with external user - simplified test without parsing
        new Expectations(ctx) {
            {
                ctx.getSecurityIntegration();
                minTimes = 0;
                result = "ldap";

                ctx.getGlobalStateMgr();
                minTimes = 0;
                result = globalStateMgr;

                ctx.setGroups((Set<String>) any);
                minTimes = 1;

                ctx.setCurrentRoleIds((UserIdentity) any, (Set<String>) any);
                minTimes = 1;
            }
        };
        ctx.setGlobalStateMgr(globalStateMgr);

        // Create external user directly for testing
        UserRef externalUserRef = new UserRef("alice", "%", false, true, NodePosition.ZERO);
        ExecuteAsStmt stmt = new ExecuteAsStmt(externalUserRef, false);

        Assertions.assertEquals("alice", stmt.getToUser().getUser());
        Assertions.assertTrue(stmt.getToUser().isExternal());
        Assertions.assertEquals("EXECUTE AS 'alice'@'%' WITH NO REVERT", stmt.toString());

        // Execute the statement
        ExecuteAsExecutor.execute(stmt, ctx);

        // Verify that the user identity is set as ephemeral (external)
        UserIdentity currentUser = ctx.getCurrentUserIdentity();
        Assertions.assertEquals("alice", currentUser.getUser());
        Assertions.assertTrue(currentUser.isEphemeral());
    }

    @Test
    public void testNativeUserImpersonate() throws Exception {
        ConnectContext ctx = new ConnectContext();
        // Test EXECUTE AS with native user (non-external) - simplified test without parsing
        new Expectations(ctx) {
            {
                ctx.getSecurityIntegration();
                minTimes = 0;
                result = "native";

                ctx.getGlobalStateMgr();
                minTimes = 0;
                result = globalStateMgr;

                ctx.setGroups((Set<String>) any);
                minTimes = 0; // Should not be called for native users

                ctx.setCurrentRoleIds((UserIdentity) any, (Set<String>) any);
                minTimes = 0; // Should not be called for native users

                ctx.updateByUserProperty((UserProperty) any);
                minTimes = 1;
            }
        };
        
        new Expectations(auth) {
            {
                auth.getUserProperty(anyString);
                minTimes = 0;
                result = new UserProperty();
            }
        };
        
        ctx.setGlobalStateMgr(globalStateMgr);

        // Create native user directly for testing
        UserRef nativeUserRef = new UserRef("user1", "%", false, false, NodePosition.ZERO);
        ExecuteAsStmt stmt = new ExecuteAsStmt(nativeUserRef, false);
        
        Assertions.assertEquals("user1", stmt.getToUser().getUser());
        Assertions.assertFalse(stmt.getToUser().isExternal());
        Assertions.assertEquals("EXECUTE AS 'user1'@'%' WITH NO REVERT", stmt.toString());

        // Execute the statement
        ExecuteAsExecutor.execute(stmt, ctx);

        // Verify that the user identity is set as non-ephemeral (native)
        UserIdentity currentUser = ctx.getCurrentUserIdentity();
        Assertions.assertEquals("user1", currentUser.getUser());
        Assertions.assertFalse(currentUser.isEphemeral());
    }
}
