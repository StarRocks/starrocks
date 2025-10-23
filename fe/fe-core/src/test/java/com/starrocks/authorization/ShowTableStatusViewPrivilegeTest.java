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
import com.starrocks.common.DdlException;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.ShowTableStatusStmt;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

/**
 * Unit tests for ShowTableStatusExecutor to verify view privilege checking
 * This test covers the bugfix: [BugFix] Fix show table status not check view privilege (#53811)
 * 
 * The main issue was that SHOW TABLE STATUS was using checkAnyActionOnTable() 
 * instead of checkAnyActionOnTableLikeObject() which properly handles views.
 */
public class ShowTableStatusViewPrivilegeTest {
    
    @BeforeEach
    public void setUp() throws Exception {
        // Mock EditLog
        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        MockedLocalMetaStore localMetastore = new MockedLocalMetaStore(globalStateMgr, globalStateMgr.getRecycleBin(), null);
        localMetastore.init();
        globalStateMgr.setLocalMetastore(localMetastore);

        RBACMockedMetadataMgr metadataMgr =
                new RBACMockedMetadataMgr(localMetastore, globalStateMgr.getConnectorMgr());
        globalStateMgr.setMetadataMgr(metadataMgr);

        new MockUp<com.starrocks.authentication.LDAPGroupProvider>() {
            @Mock
            public void init() throws DdlException {
                // do nothing
            }
        };
    }

    /**
     * Test case: Show table status without view privileges
     * Test point: Verify that SHOW TABLE STATUS properly filters out views when user lacks privileges
     * This is the core test for the bugfix in #53811
     */
    @Test
    public void testShowTableStatusWithoutViewPrivileges() throws Exception {
        // Setup authorization and authentication
        AuthorizationMgr authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);

        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        // Create test user
        UserRef testUser = new UserRef("test_user", "%", false, false, NodePosition.ZERO);
        authenticationMgr.createUser(
                new CreateUserStmt(testUser, true, null, List.of(), Map.of(), NodePosition.ZERO));

        ConnectContext ctx = new ConnectContext();
        UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
        ctx.setCurrentUserIdentity(userIdentity);
        ctx.setCurrentRoleIds(userIdentity);

        // Grant privileges to user - only table privileges, no view privileges
        String sql = "grant SELECT on TABLE db.tbl0 to test_user";
        GrantPrivilegeStmt grantPrivilegeStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(grantPrivilegeStmt, ctx);

        // Create ShowTableStatusStmt
        ShowTableStatusStmt stmt = new ShowTableStatusStmt("db", null, null);

        // Execute SHOW TABLE STATUS
        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        // Verify that only tables with proper privileges are shown
        Assertions.assertNotNull(resultSet, "Result set should not be null");
        
        // Check that the result contains only the table the user has access to
        String resultString = resultSet.getResultRows().toString();
        Assertions.assertTrue(resultString.contains("tbl0"), 
                "Should contain tbl0 which user has privileges on: " + resultString);
        
        // The view should not be visible to the user without proper privileges
        // This verifies that the bugfix is working - checkAnyActionOnTableLikeObject is being called
        Assertions.assertFalse(resultString.contains("view1"), 
                "Should not contain view1 which user lacks privileges on: " + resultString);
    }

    /**
     * Test case: Show table status with view privileges granted
     * Test point: Verify that when user has view privileges, views are shown in SHOW TABLE STATUS
     */
    @Test
    public void testShowTableStatusWithViewPrivileges() throws Exception {
        // Setup authorization and authentication
        AuthorizationMgr authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);

        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        // Create test user
        UserRef testUser = new UserRef("test_user_with_view_priv", "%", false, false, NodePosition.ZERO);
        authenticationMgr.createUser(
                new CreateUserStmt(testUser, true, null, List.of(), Map.of(), NodePosition.ZERO));

        ConnectContext ctx = new ConnectContext();
        UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp("test_user_with_view_priv", "%");
        ctx.setCurrentUserIdentity(userIdentity);
        ctx.setCurrentRoleIds(userIdentity);

        // Grant privileges to user - both table and view privileges
        String sql = "grant SELECT on TABLE db.tbl0 to test_user_with_view_priv";
        GrantPrivilegeStmt grantPrivilegeStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(grantPrivilegeStmt, ctx);

        sql = "grant SELECT on VIEW db.view1 to test_user_with_view_priv";
        grantPrivilegeStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(grantPrivilegeStmt, ctx);

        // Create ShowTableStatusStmt
        ShowTableStatusStmt stmt = new ShowTableStatusStmt("db", null, null);

        // Execute SHOW TABLE STATUS
        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        // Verify that both table and view are shown when user has proper privileges
        Assertions.assertNotNull(resultSet, "Result set should not be null");
        
        String resultString = resultSet.getResultRows().toString();
        Assertions.assertTrue(resultString.contains("tbl0"), 
                "Should contain tbl0 which user has privileges on: " + resultString);
        Assertions.assertTrue(resultString.contains("view1"), 
                "Should contain view1 which user has privileges on: " + resultString);
    }

    /**
     * Test case: Show table status with materialized view privilege check
     * Test point: Verify that SHOW TABLE STATUS properly checks materialized view privileges
     */
    @Test
    public void testShowTableStatusWithMaterializedViewPrivileges() throws Exception {
        // Setup authorization and authentication
        AuthorizationMgr authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);

        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        // Create test user
        UserRef testUser = new UserRef("test_user_mv", "%", false, false, NodePosition.ZERO);
        authenticationMgr.createUser(
                new CreateUserStmt(testUser, true, null, List.of(), Map.of(), NodePosition.ZERO));

        ConnectContext ctx = new ConnectContext();
        UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp("test_user_mv", "%");
        ctx.setCurrentUserIdentity(userIdentity);
        ctx.setCurrentRoleIds(userIdentity);

        // Grant privileges to user - only table privileges, no materialized view privileges
        String sql = "grant SELECT on TABLE db.tbl0 to test_user_mv";
        GrantPrivilegeStmt grantPrivilegeStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(grantPrivilegeStmt, ctx);

        // Create ShowTableStatusStmt
        ShowTableStatusStmt stmt = new ShowTableStatusStmt("db", null, null);

        // Execute SHOW TABLE STATUS
        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        // Verify that materialized view is not shown when user lacks privileges
        Assertions.assertNotNull(resultSet, "Result set should not be null");
        
        String resultString = resultSet.getResultRows().toString();
        Assertions.assertTrue(resultString.contains("tbl0"), 
                "Should contain tbl0 which user has privileges on: " + resultString);
        
        // The materialized view should not be visible to the user without proper privileges
        Assertions.assertFalse(resultString.contains("mv1"), 
                "Should not contain mv1 which user lacks privileges on: " + resultString);
    }

    /**
     * Test case: Show table status with no privileges
     * Test point: Verify that SHOW TABLE STATUS returns empty result when user has no privileges
     */
    @Test
    public void testShowTableStatusWithNoPrivileges() throws Exception {
        // Setup authorization and authentication
        AuthorizationMgr authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);

        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        // Create test user with no privileges
        UserRef testUser = new UserRef("test_user_no_priv", "%", false, false, NodePosition.ZERO);
        authenticationMgr.createUser(
                new CreateUserStmt(testUser, true, null, List.of(), Map.of(), NodePosition.ZERO));

        ConnectContext ctx = new ConnectContext();
        UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp("test_user_no_priv", "%");
        ctx.setCurrentUserIdentity(userIdentity);
        ctx.setCurrentRoleIds(userIdentity);

        // Create ShowTableStatusStmt
        ShowTableStatusStmt stmt = new ShowTableStatusStmt("db", null, null);

        // Execute SHOW TABLE STATUS
        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        // Verify that no tables/views are shown when user has no privileges
        Assertions.assertNotNull(resultSet, "Result set should not be null");
        Assertions.assertTrue(resultSet.getResultRows().isEmpty(), 
                "Should return empty result when user has no privileges");
    }
}