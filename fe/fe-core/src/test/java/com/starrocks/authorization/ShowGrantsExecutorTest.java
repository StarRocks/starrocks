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
import com.starrocks.authentication.LDAPGroupProvider;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorReportException;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.GrantType;
import com.starrocks.sql.ast.ShowGrantsStmt;
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

public class ShowGrantsExecutorTest {
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

        new MockUp<LDAPGroupProvider>() {
            @Mock
            public void init() throws DdlException {
                // do nothing
            }
        };
    }

    /**
     * Test case: Show grants for root role
     * Test point: Verify that root role has all necessary privileges including CREATE TABLE, DROP, ALTER, etc.
     */
    @Test
    public void testShowGrantsForRootRole() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(UserIdentity.ROOT);

        ShowGrantsStmt stmt = new ShowGrantsStmt("root", GrantType.ROLE, NodePosition.ZERO);

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        resultSet.getResultRows().forEach(System.out::println);
        String expectString1 = "root, null, GRANT CREATE TABLE, DROP, ALTER, CREATE VIEW, CREATE FUNCTION, " +
                "CREATE MATERIALIZED VIEW, CREATE PIPE ON ALL DATABASES TO ROLE 'root'";
        Assertions.assertTrue(resultSet.getResultRows().stream().anyMatch(l -> l.toString().contains(expectString1)));
        String expectString2 = "root, null, GRANT DELETE, DROP, INSERT, SELECT, ALTER, EXPORT, " +
                "UPDATE, REFRESH ON ALL TABLES IN ALL DATABASES TO ROLE 'root'";
        Assertions.assertTrue(resultSet.getResultRows().stream().anyMatch(l -> l.toString().contains(expectString2)));
    }

    /**
     * Test case: Show grants privilege check
     * Test point: Verify that users without proper privileges cannot show grants for roles they don't have access to
     */
    @Test
    public void testShowGrantsPrivilegeCheck() throws Exception {
        AuthorizationMgr authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);

        CreateRoleStmt createRoleStmt = new CreateRoleStmt(List.of("r_show_grants"), false, null);
        authorizationMgr.createRole(createRoleStmt);

        ConnectContext ctx = new ConnectContext();
        UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
        ctx.setCurrentUserIdentity(testUser);
        ctx.setCurrentRoleIds(testUser);

        try {
            ShowGrantsStmt showGrantsStmt = new ShowGrantsStmt("r_show_grants", GrantType.ROLE, NodePosition.ZERO);
            Authorizer.check(showGrantsStmt, ctx);
            Assertions.fail();
        } catch (ErrorReportException e) {
            Assertions.assertTrue(e.getMessage().contains("Access denied"));
        }
    }

    /**
     * Test case: Show grants for user and role with complex privilege scenarios
     * Test point: Verify that user privileges, role privileges, and role inheritance are displayed correctly
     */
    @Test
    public void testShowGrantsForUserAndRoleWithComplexPrivileges() throws Exception {
        AuthorizationMgr authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);

        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        authenticationMgr.createUser(
                new CreateUserStmt(new UserRef("u1", "%"), true, null, List.of(), Map.of(), NodePosition.ZERO));

        authorizationMgr.createRole(new CreateRoleStmt(List.of("r0"), false, null));
        authorizationMgr.createRole(new CreateRoleStmt(List.of("r1"), false, null));
        authorizationMgr.createRole(new CreateRoleStmt(List.of("r2"), false, null));

        ConnectContext ctx = new ConnectContext();

        String sql = "grant all on CATALOG default_catalog to u1";
        GrantPrivilegeStmt grantPrivilegeStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(grantPrivilegeStmt, ctx);

        ShowGrantsStmt stmt = new ShowGrantsStmt(new UserRef("u1", "%"), NodePosition.ZERO);

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals("[['u1'@'%', default_catalog, GRANT USAGE, CREATE DATABASE, DROP, ALTER " +
                "ON CATALOG default_catalog TO USER 'u1'@'%']]", resultSet.getResultRows().toString());

        sql = "grant all on CATALOG default_catalog to role r1";
        grantPrivilegeStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(grantPrivilegeStmt, ctx);

        stmt = new ShowGrantsStmt("r1", GrantType.ROLE, NodePosition.ZERO);
        resultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals("[[r1, default_catalog, GRANT USAGE, CREATE DATABASE, DROP, ALTER " +
                "ON CATALOG default_catalog TO ROLE 'r1']]", resultSet.getResultRows().toString());

        sql = "grant r1 to role r0";
        GrantRoleStmt grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(grantRoleStmt, ctx);

        sql = "grant r2 to role r0";
        grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(grantRoleStmt, ctx);

        sql = "grant SELECT on TABLE db.tbl0 to role r0";
        grantPrivilegeStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(grantPrivilegeStmt, ctx);

        stmt = new ShowGrantsStmt("r0", GrantType.ROLE, NodePosition.ZERO);
        resultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals("[[r0, null, GRANT 'r1', 'r2' TO ROLE r0]," +
                        " [r0, default_catalog, GRANT SELECT ON TABLE db.tbl0 TO ROLE 'r0']]",
                resultSet.getResultRows().toString());
    }

    /**
     * Test case: Show grants for external user
     * Test point: Verify that external users not throw exceptions and return empty results
     */
    @Test
    public void testShowGrantsForExternalUser() {
        AuthorizationMgr authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);

        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(UserIdentity.createEphemeralUserIdent("external_user", "%"));

        // Test showing grants for external user
        UserRef externalUser = new UserRef("external_user", "%", false, true, NodePosition.ZERO);
        ShowGrantsStmt stmt = new ShowGrantsStmt(externalUser, NodePosition.ZERO);
        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertTrue(resultSet.getResultRows().isEmpty());

        stmt = new ShowGrantsStmt(null, NodePosition.ZERO);
        resultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertTrue(resultSet.getResultRows().isEmpty());
    }

    /**
     * Test case: Show grants for user with domain
     * Test point: Verify that users with domain specification are handled correctly
     */
    @Test
    public void testShowGrantsForUserWithDomain() throws Exception {
        AuthorizationMgr authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);

        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        // Create user with domain - use the correct format for domain user
        UserRef domainUser = new UserRef("domain_user", "example.com", true, false, NodePosition.ZERO);
        authenticationMgr.createUser(
                new CreateUserStmt(domainUser, true, null, List.of(), Map.of(), NodePosition.ZERO));

        ConnectContext ctx = new ConnectContext();

        // Grant privileges to domain user using the correct user reference format
        String sql = "grant SELECT on ALL TABLES IN ALL DATABASES to 'domain_user'@['example.com']";
        GrantPrivilegeStmt grantPrivilegeStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(grantPrivilegeStmt, ctx);

        // Test showing grants for domain user
        ShowGrantsStmt stmt = new ShowGrantsStmt(domainUser, NodePosition.ZERO);
        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        // Verify domain user format in result
        Assertions.assertTrue(resultSet.getResultRows().stream()
                .anyMatch(row -> row.toString().contains("'domain_user'@['example.com']")));
        Assertions.assertTrue(resultSet.getResultRows().stream()
                .anyMatch(row -> row.toString().contains("GRANT SELECT ON ALL TABLES IN ALL DATABASES")));
    }

    /**
     * Test case: Show grants for non-existent role
     * Test point: Verify that showing grants for non-existent role throws exception
     */
    @Test
    public void testShowGrantsForNonExistentRole() throws Exception {
        AuthorizationMgr authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);

        ConnectContext ctx = new ConnectContext();

        // Test showing grants for non-existent role should throw exception
        ShowGrantsStmt stmt = new ShowGrantsStmt("non_existent_role", GrantType.ROLE, NodePosition.ZERO);
        
        try {
            ShowExecutor.execute(stmt, ctx);
            Assertions.fail("Expected SemanticException for non-existent role");
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("cannot find role non_existent_role"));
        }
    }

    /**
     * Test case: Show grants for user with multiple privilege types
     * Test point: Verify that different privilege types (CATALOG, DATABASE, TABLE) are displayed correctly
     */
    @Test
    public void testShowGrantsForUserWithMultiplePrivilegeTypes() throws Exception {
        AuthorizationMgr authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);

        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        // Create user
        UserRef multiPrivUser = new UserRef("multi_priv_user", "%", false, false, NodePosition.ZERO);
        authenticationMgr.createUser(
                new CreateUserStmt(multiPrivUser, true, null, List.of(), Map.of(), NodePosition.ZERO));

        ConnectContext ctx = new ConnectContext();

        // Grant catalog privileges
        String sql = "grant USAGE on CATALOG default_catalog to multi_priv_user";
        GrantPrivilegeStmt grantPrivilegeStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(grantPrivilegeStmt, ctx);

        // Grant database privileges - use correct privilege type for database
        sql = "grant DROP on ALL DATABASES to multi_priv_user";
        grantPrivilegeStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(grantPrivilegeStmt, ctx);

        // Grant table privileges
        sql = "grant SELECT, INSERT on ALL TABLES IN ALL DATABASES to multi_priv_user";
        grantPrivilegeStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(grantPrivilegeStmt, ctx);

        // Test showing grants
        ShowGrantsStmt stmt = new ShowGrantsStmt(multiPrivUser, NodePosition.ZERO);
        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        // Verify all privilege types are present
        String resultString = resultSet.getResultRows().toString();
        Assertions.assertTrue(resultString.contains("GRANT USAGE ON CATALOG default_catalog"),
                "Should contain catalog privilege: " + resultString);
        Assertions.assertTrue(resultString.contains("GRANT DROP ON ALL DATABASES"),
                "Should contain database privilege: " + resultString);
        Assertions.assertTrue(resultString.contains("GRANT INSERT, SELECT ON ALL TABLES IN ALL DATABASES"),
                "Should contain table privilege: " + resultString);
    }

    /**
     * Test case: Show grants for role with WITH GRANT OPTION
     * Test point: Verify that WITH GRANT OPTION is properly displayed in grant statements
     */
    @Test
    public void testShowGrantsWithGrantOption() throws Exception {
        AuthorizationMgr authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);

        // Create role
        authorizationMgr.createRole(new CreateRoleStmt(List.of("grant_option_role"), false, null));

        ConnectContext ctx = new ConnectContext();

        // Grant privileges with WITH GRANT OPTION
        String sql = "grant SELECT on ALL TABLES IN ALL DATABASES to role grant_option_role WITH GRANT OPTION";
        GrantPrivilegeStmt grantPrivilegeStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(grantPrivilegeStmt, ctx);

        // Test showing grants
        ShowGrantsStmt stmt = new ShowGrantsStmt("grant_option_role", GrantType.ROLE, NodePosition.ZERO);
        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        // Verify WITH GRANT OPTION is displayed
        Assertions.assertTrue(resultSet.getResultRows().stream()
                .anyMatch(row -> row.toString().contains("WITH GRANT OPTION")));
    }

    /**
     * Test case: Show grants for user with no privileges
     * Test point: Verify that user with no privileges returns empty result
     */
    @Test
    public void testShowGrantsForUserWithNoPrivileges() throws Exception {
        AuthorizationMgr authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);

        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        // Create user without any privileges
        UserRef noPrivUser = new UserRef("no_priv_user", "%", false, false, NodePosition.ZERO);
        authenticationMgr.createUser(
                new CreateUserStmt(noPrivUser, true, null, List.of(), Map.of(), NodePosition.ZERO));

        ConnectContext ctx = new ConnectContext();

        // Test showing grants for user with no privileges
        ShowGrantsStmt stmt = new ShowGrantsStmt(noPrivUser, NodePosition.ZERO);
        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        // Should return empty result for user with no privileges
        Assertions.assertTrue(resultSet.getResultRows().isEmpty());
    }
}
