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

import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.GrantType;
import com.starrocks.sql.ast.ShowGrantsStmt;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AuthorizationAnalyzerTest {
    static ConnectContext ctx;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        ctx.getGlobalStateMgr().getAuthorizationMgr().initBuiltinRolesAndUsers();
    }

    @Test
    public void testVisitShowGrantsStatementWithNullUser() {
        // Test case 1: stmt.getUser() == null, should skip existence check
        ShowGrantsStmt stmt = new ShowGrantsStmt((UserRef) null, NodePosition.ZERO);
        Assertions.assertNull(stmt.getUser());
        Assertions.assertEquals(GrantType.USER, stmt.getGrantType());
        ctx.setCurrentUserIdentity(new UserIdentity("u1", "%"));

        // This should not throw any exception and should not call checkUserExist
        AuthorizationAnalyzer.analyze(stmt, ctx);
        
        // Verify that the user is set to current user identity
        Assertions.assertNotNull(stmt.getUser());
        Assertions.assertEquals("u1", stmt.getUser().getUser());
        Assertions.assertEquals("%", stmt.getUser().getHost());
    }

    @Test
    public void testVisitShowGrantsStatementWithExplicitUser() throws Exception {
        // Test case 2: stmt.getUser() != null, should perform existence check
        UserRef explicitUser = new UserRef("test_user", "%");
        ShowGrantsStmt stmt = new ShowGrantsStmt(explicitUser, NodePosition.ZERO);
        Assertions.assertNotNull(stmt.getUser());
        Assertions.assertEquals("test_user", stmt.getUser().getUser());
        Assertions.assertEquals(GrantType.USER, stmt.getGrantType());

        // This should throw an exception because test_user doesn't exist
        Assertions.assertThrows(SemanticException.class, () -> {
            AuthorizationAnalyzer.analyze(stmt, ctx);
        });
    }

    @Test
    public void testVisitShowGrantsStatementWithRole() throws Exception {
        // Test case 3: GrantType.ROLE, should validate role name
        ShowGrantsStmt stmt = new ShowGrantsStmt("test_role", GrantType.ROLE, NodePosition.ZERO);
        Assertions.assertEquals("test_role", stmt.getGroupOrRole());
        Assertions.assertEquals(GrantType.ROLE, stmt.getGrantType());

        // This should throw an exception because test_role doesn't exist
        Assertions.assertThrows(SemanticException.class, () -> {
            AuthorizationAnalyzer.analyze(stmt, ctx);
        });
    }

    @Test
    public void testVisitShowGrantsStatementWithExistingRole() throws Exception {
        // Test case 4: GrantType.ROLE with existing role
        ShowGrantsStmt stmt = new ShowGrantsStmt("root", GrantType.ROLE, NodePosition.ZERO);
        Assertions.assertEquals("root", stmt.getGroupOrRole());
        Assertions.assertEquals(GrantType.ROLE, stmt.getGrantType());

        // This should not throw an exception because root role exists
        AuthorizationAnalyzer.analyze(stmt, ctx);
    }
}