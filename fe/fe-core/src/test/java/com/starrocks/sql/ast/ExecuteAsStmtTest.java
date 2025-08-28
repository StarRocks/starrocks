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
import com.starrocks.sql.parser.SqlParser;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExecuteAsStmtTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private AuthenticationMgr auth;
    @Mocked
    private AuthorizationMgr authorizationMgr;
    @Mocked
    private ConnectContext ctx;

    @BeforeEach
    public void setUp() throws PrivilegeException {

        SqlParser sqlParser = new SqlParser(AstBuilder.getInstance());
        Analyzer analyzer = new Analyzer(Analyzer.AnalyzerVisitor.getInstance());
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getAuthenticationMgr();
                minTimes = 0;
                result = auth;

                GlobalStateMgr.getCurrentState().getAuthorizationMgr().getDefaultRoleIdsByUser((UserIdentity) any);
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

        new Expectations(ctx) {
            {
                ctx.updateByUserProperty((UserProperty) any);
                minTimes = 0;
            }
        };
        ctx.setGlobalStateMgr(globalStateMgr);

        ExecuteAsStmt stmt = (ExecuteAsStmt) com.starrocks.sql.parser.SqlParser.parse(
                "execute as user1 with no revert", 1).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assertions.assertEquals("user1", stmt.getToUser().getUser());
        Assertions.assertEquals("%", stmt.getToUser().getHost());
        Assertions.assertEquals("EXECUTE AS 'user1'@'%' WITH NO REVERT", stmt.toString());
        Assertions.assertFalse(stmt.isAllowRevert());

        ExecuteAsExecutor.execute(stmt, ctx);

        Assertions.assertEquals(new UserIdentity("user1", "%"), ctx.getCurrentUserIdentity());
    }

    @Test
    public void testUserNotExist() {
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
}
