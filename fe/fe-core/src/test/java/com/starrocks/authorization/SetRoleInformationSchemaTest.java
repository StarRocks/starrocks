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
import com.starrocks.authentication.UserIdentityUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SetRoleExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TDescribeTableParams;
import com.starrocks.thrift.TDescribeTableResult;
import com.starrocks.thrift.TGetDbsParams;
import com.starrocks.thrift.TGetDbsResult;
import com.starrocks.thrift.TGetTablesParams;
import com.starrocks.thrift.TGetTablesResult;
import com.starrocks.thrift.TListTableStatusResult;
import com.starrocks.thrift.TTableStatus;
import com.starrocks.thrift.TUserIdentity;
import com.starrocks.thrift.TUserRoles;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Integration tests for SET ROLE propagation to information_schema queries.
 *
 * Tests the bugfix: SET ROLE not propagating to all information_schema paths.
 *
 * When activate_all_roles_on_login=OFF, SET ROLE updates the session's active
 * role IDs. These tests verify the role IDs are correctly propagated through
 * all thrift handlers and system table query methods, so that authorization
 * checks reflect the active role rather than all granted roles.
 *
 * Tested paths:
 * - FrontendServiceImpl.getDbNames() — DB visibility
 * - FrontendServiceImpl.getTableNames() — table visibility
 * - FrontendServiceImpl.describeTable() — column visibility
 * - FrontendServiceImpl.listTableStatus() → ViewsSystemTable.query() — table listing
 */
public class SetRoleInformationSchemaTest {
    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;
    private static final String DB_1 = "test_set_role_db1";
    private static final String DB_2 = "test_set_role_db2";
    private static final String TABLE_1 = "tbl1";
    private static final String TABLE_2 = "tbl2";
    private static final String ROLE_1 = "test_sr_role_1";
    private static final String ROLE_2 = "test_sr_role_2";
    private static final String TEST_USER = "test_set_role_user";
    private static FrontendServiceImpl frontendService;
    private static long role1Id;
    private static long role2Id;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        ctx.setThreadLocalInfo();
        starRocksAssert = new StarRocksAssert(ctx);
        frontendService = new FrontendServiceImpl(null);

        // Disable activate_all_roles_on_login — this is the scenario where the bug manifests
        GlobalVariable.setActivateAllRolesOnLogin(false);

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        globalStateMgr.setAuthenticationMgr(new AuthenticationMgr());
        globalStateMgr.setAuthorizationMgr(new AuthorizationMgr(new DefaultAuthorizationProvider()));

        // Create two databases, each with one table
        starRocksAssert.withDatabase(DB_1).useDatabase(DB_1);
        starRocksAssert.withTable("create table " + TABLE_1 + " (id int) properties('replication_num'='1')");

        starRocksAssert.withDatabase(DB_2).useDatabase(DB_2);
        starRocksAssert.withTable("create table " + TABLE_2 + " (id int) properties('replication_num'='1')");

        // Create test user
        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user " + TEST_USER, ctx);
        globalStateMgr.getAuthenticationMgr().createUser(createUserStmt);

        // Create roles
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create role " + ROLE_1, ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create role " + ROLE_2, ctx), ctx);

        // Grant role1: access to db1.tbl1
        GrantPrivilegeStmt grantStmt1 = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "grant select on " + DB_1 + "." + TABLE_1 + " to role " + ROLE_1, ctx);
        globalStateMgr.getAuthorizationMgr().grant(grantStmt1);

        // Grant role2: access to db2.tbl2
        GrantPrivilegeStmt grantStmt2 = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "grant select on " + DB_2 + "." + TABLE_2 + " to role " + ROLE_2, ctx);
        globalStateMgr.getAuthorizationMgr().grant(grantStmt2);

        // Grant both roles to test user
        GrantRoleStmt grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                "grant " + ROLE_1 + ", " + ROLE_2 + " to " + TEST_USER, ctx);
        globalStateMgr.getAuthorizationMgr().grantRole(grantRoleStmt);

        // Cache role IDs
        role1Id = globalStateMgr.getAuthorizationMgr().getRoleIdByNameNoLock(ROLE_1);
        role2Id = globalStateMgr.getAuthorizationMgr().getRoleIdByNameNoLock(ROLE_2);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        starRocksAssert.dropDatabase(DB_1);
        starRocksAssert.dropDatabase(DB_2);
    }

    // -----------------------------------------------------------------------
    // Helper methods
    // -----------------------------------------------------------------------

    private static TUserIdentity buildUserIdentityWithRoles(UserIdentity user, Set<Long> roleIds) {
        TUserIdentity tUserIdentity = UserIdentityUtils.toThrift(user);
        if (roleIds != null) {
            TUserRoles tUserRoles = new TUserRoles();
            tUserRoles.setRole_id_list(new ArrayList<>(roleIds));
            tUserIdentity.setCurrent_role_ids(tUserRoles);
        }
        return tUserIdentity;
    }

    private static UserIdentity getTestUserIdentity() {
        return UserIdentity.createAnalyzedUserIdentWithIp(TEST_USER, "%");
    }

    // -----------------------------------------------------------------------
    // Unit tests: SET ROLE mechanics
    // -----------------------------------------------------------------------

    @Test
    public void testSetRoleUpdatesContextRoleIds() throws Exception {
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(UserIdentity.ROOT);

        // Switch to test user
        UserIdentity testUser = getTestUserIdentity();
        ctx.setCurrentUserIdentity(testUser);
        ctx.setCurrentRoleIds(testUser);

        // SET ROLE role1
        SetRoleStmt setRoleStmt = (SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                "set role " + ROLE_1, ctx);
        SetRoleExecutor.execute(setRoleStmt, ctx);

        Assertions.assertTrue(ctx.getCurrentRoleIds().contains(role1Id),
                "Context should contain role1 ID after SET ROLE role1");
        Assertions.assertEquals(1, ctx.getCurrentRoleIds().size(),
                "Context should have exactly 1 role ID after SET ROLE role1");

        // SET ROLE role2
        setRoleStmt = (SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                "set role " + ROLE_2, ctx);
        SetRoleExecutor.execute(setRoleStmt, ctx);

        Assertions.assertTrue(ctx.getCurrentRoleIds().contains(role2Id),
                "Context should contain role2 ID after SET ROLE role2");
        Assertions.assertEquals(1, ctx.getCurrentRoleIds().size(),
                "Context should have exactly 1 role ID after SET ROLE role2");

        // SET ROLE ALL
        setRoleStmt = (SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                "set role all", ctx);
        SetRoleExecutor.execute(setRoleStmt, ctx);

        Assertions.assertTrue(ctx.getCurrentRoleIds().contains(role1Id) &&
                        ctx.getCurrentRoleIds().contains(role2Id),
                "Context should contain both role IDs after SET ROLE ALL");
    }

    @Test
    public void testSetRoleNoneProducesEmptyRoleIds() throws Exception {
        UserIdentity testUser = getTestUserIdentity();
        ctx.setCurrentUserIdentity(testUser);
        ctx.setCurrentRoleIds(testUser);

        // SET ROLE NONE
        SetRoleStmt setRoleStmt = (SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                "set role none", ctx);
        SetRoleExecutor.execute(setRoleStmt, ctx);

        Assertions.assertNotNull(ctx.getCurrentRoleIds(),
                "SET ROLE NONE should produce non-null role set");
        Assertions.assertTrue(ctx.getCurrentRoleIds().isEmpty(),
                "SET ROLE NONE should produce empty role set");
    }

    @Test
    public void testSetRoleNoneSerializationRoundTrip() throws Exception {
        UserIdentity testUser = getTestUserIdentity();

        // Build TUserIdentity with empty role set (SET ROLE NONE)
        TUserIdentity tUserIdentity = buildUserIdentityWithRoles(testUser, Set.of());

        Assertions.assertTrue(tUserIdentity.isSetCurrent_role_ids(),
                "Empty role set should still be serialized as current_role_ids");

        // Deserialize and verify
        ConnectContext tempCtx = new ConnectContext();
        UserIdentityUtils.setAuthInfoFromThrift(tempCtx, tUserIdentity);

        Assertions.assertNotNull(tempCtx.getCurrentRoleIds(),
                "Deserialized role IDs should not be null");
        Assertions.assertTrue(tempCtx.getCurrentRoleIds().isEmpty(),
                "Deserialized role IDs should be empty for SET ROLE NONE");
    }

    @Test
    public void testRoleIdPropagation() throws Exception {
        UserIdentity testUser = getTestUserIdentity();
        ctx.setCurrentUserIdentity(testUser);
        ctx.setCurrentRoleIds(testUser);

        SetRoleStmt setRoleStmt = (SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                "set role " + ROLE_1, ctx);
        SetRoleExecutor.execute(setRoleStmt, ctx);

        Assertions.assertFalse(ctx.getCurrentRoleIds().isEmpty(),
                "ConnectContext should have role IDs after SET ROLE");

        TUserIdentity tUserIdentity = buildUserIdentityWithRoles(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds());

        Assertions.assertTrue(tUserIdentity.isSetCurrent_role_ids(),
                "TUserIdentity should have current_role_ids set");
        Assertions.assertFalse(tUserIdentity.getCurrent_role_ids().getRole_id_list().isEmpty(),
                "TUserIdentity role_id_list should not be empty");
    }

    // -----------------------------------------------------------------------
    // Integration tests: getDbNames
    // -----------------------------------------------------------------------

    @Test
    public void testGetDbNamesRespectsSetRole() throws Exception {
        UserIdentity testUser = getTestUserIdentity();

        // With role1 only: should see db1 but not db2
        TGetDbsParams params1 = new TGetDbsParams();
        params1.setCurrent_user_ident(buildUserIdentityWithRoles(testUser, Set.of(role1Id)));
        TGetDbsResult result1 = frontendService.getDbNames(params1);
        List<String> dbs1 = result1.getDbs();

        Assertions.assertTrue(dbs1.contains(DB_1),
                "With role1, should see " + DB_1 + ", got: " + dbs1);
        Assertions.assertFalse(dbs1.contains(DB_2),
                "With role1, should NOT see " + DB_2 + ", got: " + dbs1);

        // With role2 only: should see db2 but not db1
        TGetDbsParams params2 = new TGetDbsParams();
        params2.setCurrent_user_ident(buildUserIdentityWithRoles(testUser, Set.of(role2Id)));
        TGetDbsResult result2 = frontendService.getDbNames(params2);
        List<String> dbs2 = result2.getDbs();

        Assertions.assertTrue(dbs2.contains(DB_2),
                "With role2, should see " + DB_2 + ", got: " + dbs2);
        Assertions.assertFalse(dbs2.contains(DB_1),
                "With role2, should NOT see " + DB_1 + ", got: " + dbs2);

        // With both roles: should see both
        TGetDbsParams params3 = new TGetDbsParams();
        params3.setCurrent_user_ident(buildUserIdentityWithRoles(testUser, Set.of(role1Id, role2Id)));
        TGetDbsResult result3 = frontendService.getDbNames(params3);
        List<String> dbs3 = result3.getDbs();

        Assertions.assertTrue(dbs3.contains(DB_1) && dbs3.contains(DB_2),
                "With both roles, should see both databases, got: " + dbs3);
    }

    // -----------------------------------------------------------------------
    // Integration tests: getTableNames
    // -----------------------------------------------------------------------

    @Test
    public void testGetTableNamesRespectsSetRole() throws Exception {
        UserIdentity testUser = getTestUserIdentity();

        // With role1: should see tbl1 in db1
        TGetTablesParams params1 = new TGetTablesParams();
        params1.setCurrent_user_ident(buildUserIdentityWithRoles(testUser, Set.of(role1Id)));
        params1.setDb(DB_1);
        TGetTablesResult result1 = frontendService.getTableNames(params1);

        Assertions.assertTrue(result1.getTables().contains(TABLE_1),
                "With role1, should see " + TABLE_1 + " in " + DB_1);

        // With role2: should NOT see tbl1 in db1
        TGetTablesParams params2 = new TGetTablesParams();
        params2.setCurrent_user_ident(buildUserIdentityWithRoles(testUser, Set.of(role2Id)));
        params2.setDb(DB_1);
        TGetTablesResult result2 = frontendService.getTableNames(params2);

        Assertions.assertFalse(result2.getTables().contains(TABLE_1),
                "With role2, should NOT see " + TABLE_1 + " in " + DB_1 + ", got: " + result2.getTables());

        // With role2: should see tbl2 in db2
        TGetTablesParams params3 = new TGetTablesParams();
        params3.setCurrent_user_ident(buildUserIdentityWithRoles(testUser, Set.of(role2Id)));
        params3.setDb(DB_2);
        TGetTablesResult result3 = frontendService.getTableNames(params3);

        Assertions.assertTrue(result3.getTables().contains(TABLE_2),
                "With role2, should see " + TABLE_2 + " in " + DB_2);

        // With role1: should NOT see tbl2 in db2
        TGetTablesParams params4 = new TGetTablesParams();
        params4.setCurrent_user_ident(buildUserIdentityWithRoles(testUser, Set.of(role1Id)));
        params4.setDb(DB_2);
        TGetTablesResult result4 = frontendService.getTableNames(params4);

        Assertions.assertFalse(result4.getTables().contains(TABLE_2),
                "With role1, should NOT see " + TABLE_2 + " in " + DB_2 + ", got: " + result4.getTables());
    }

    // -----------------------------------------------------------------------
    // Integration tests: describeTable
    // -----------------------------------------------------------------------

    @Test
    public void testDescribeTableRespectsSetRole() throws Exception {
        UserIdentity testUser = getTestUserIdentity();

        // With role1: should see columns of tbl1 in db1
        TDescribeTableParams params1 = new TDescribeTableParams();
        params1.setCurrent_user_ident(buildUserIdentityWithRoles(testUser, Set.of(role1Id)));
        params1.setDb(DB_1);
        params1.setTable_name(TABLE_1);
        TDescribeTableResult result1 = frontendService.describeTable(params1);

        Assertions.assertFalse(result1.getColumns().isEmpty(),
                "With role1, should see columns of " + TABLE_1 + " in " + DB_1);

        // With role2: should NOT see columns of tbl1 in db1
        TDescribeTableParams params2 = new TDescribeTableParams();
        params2.setCurrent_user_ident(buildUserIdentityWithRoles(testUser, Set.of(role2Id)));
        params2.setDb(DB_1);
        params2.setTable_name(TABLE_1);
        TDescribeTableResult result2 = frontendService.describeTable(params2);

        Assertions.assertTrue(result2.getColumns().isEmpty(),
                "With role2, should NOT see columns of " + TABLE_1 + " in " + DB_1);

        // With role2: should see columns of tbl2 in db2
        TDescribeTableParams params3 = new TDescribeTableParams();
        params3.setCurrent_user_ident(buildUserIdentityWithRoles(testUser, Set.of(role2Id)));
        params3.setDb(DB_2);
        params3.setTable_name(TABLE_2);
        TDescribeTableResult result3 = frontendService.describeTable(params3);

        Assertions.assertFalse(result3.getColumns().isEmpty(),
                "With role2, should see columns of " + TABLE_2 + " in " + DB_2);

        // With role1: should NOT see columns of tbl2 in db2
        TDescribeTableParams params4 = new TDescribeTableParams();
        params4.setCurrent_user_ident(buildUserIdentityWithRoles(testUser, Set.of(role1Id)));
        params4.setDb(DB_2);
        params4.setTable_name(TABLE_2);
        TDescribeTableResult result4 = frontendService.describeTable(params4);

        Assertions.assertTrue(result4.getColumns().isEmpty(),
                "With role1, should NOT see columns of " + TABLE_2 + " in " + DB_2);
    }

    // -----------------------------------------------------------------------
    // Integration tests: listTableStatus (ViewsSystemTable.query path)
    // -----------------------------------------------------------------------

    @Test
    public void testListTableStatusRespectsSetRole() throws Exception {
        UserIdentity testUser = getTestUserIdentity();

        // With role1: should see tbl1 in db1
        TGetTablesParams params1 = new TGetTablesParams();
        params1.setCurrent_user_ident(buildUserIdentityWithRoles(testUser, Set.of(role1Id)));
        params1.setDb(DB_1);
        TListTableStatusResult result1 = frontendService.listTableStatus(params1);
        List<String> tables1 = result1.getTables().stream()
                .map(TTableStatus::getName).collect(Collectors.toList());

        Assertions.assertTrue(tables1.contains(TABLE_1),
                "With role1, should see " + TABLE_1 + " in " + DB_1 + ", got: " + tables1);

        // With role2: should NOT see tbl1 in db1
        TGetTablesParams params2 = new TGetTablesParams();
        params2.setCurrent_user_ident(buildUserIdentityWithRoles(testUser, Set.of(role2Id)));
        params2.setDb(DB_1);
        TListTableStatusResult result2 = frontendService.listTableStatus(params2);
        List<String> tables2 = result2.getTables().stream()
                .map(TTableStatus::getName).collect(Collectors.toList());

        Assertions.assertFalse(tables2.contains(TABLE_1),
                "With role2, should NOT see " + TABLE_1 + " in " + DB_1 + ", got: " + tables2);

        // With role2: should see tbl2 in db2
        TGetTablesParams params3 = new TGetTablesParams();
        params3.setCurrent_user_ident(buildUserIdentityWithRoles(testUser, Set.of(role2Id)));
        params3.setDb(DB_2);
        TListTableStatusResult result3 = frontendService.listTableStatus(params3);
        List<String> tables3 = result3.getTables().stream()
                .map(TTableStatus::getName).collect(Collectors.toList());

        Assertions.assertTrue(tables3.contains(TABLE_2),
                "With role2, should see " + TABLE_2 + " in " + DB_2 + ", got: " + tables3);

        // With role1: should NOT see tbl2 in db2
        TGetTablesParams params4 = new TGetTablesParams();
        params4.setCurrent_user_ident(buildUserIdentityWithRoles(testUser, Set.of(role1Id)));
        params4.setDb(DB_2);
        TListTableStatusResult result4 = frontendService.listTableStatus(params4);
        List<String> tables4 = result4.getTables().stream()
                .map(TTableStatus::getName).collect(Collectors.toList());

        Assertions.assertFalse(tables4.contains(TABLE_2),
                "With role1, should NOT see " + TABLE_2 + " in " + DB_2 + ", got: " + tables4);
    }

    // -----------------------------------------------------------------------
    // Integration test: SET ROLE NONE hides all metadata
    // -----------------------------------------------------------------------

    @Test
    public void testSetRoleNoneHidesAllDatabases() throws Exception {
        UserIdentity testUser = getTestUserIdentity();

        // SET ROLE NONE: empty role set — should see neither db1 nor db2
        TGetDbsParams params = new TGetDbsParams();
        params.setCurrent_user_ident(buildUserIdentityWithRoles(testUser, Set.of()));
        TGetDbsResult result = frontendService.getDbNames(params);
        List<String> dbs = result.getDbs();

        Assertions.assertFalse(dbs.contains(DB_1),
                "With SET ROLE NONE, should NOT see " + DB_1 + ", got: " + dbs);
        Assertions.assertFalse(dbs.contains(DB_2),
                "With SET ROLE NONE, should NOT see " + DB_2 + ", got: " + dbs);
    }

    @Test
    public void testSetRoleNoneHidesAllTables() throws Exception {
        UserIdentity testUser = getTestUserIdentity();

        // SET ROLE NONE: should not see tbl1 in db1
        TGetTablesParams params1 = new TGetTablesParams();
        params1.setCurrent_user_ident(buildUserIdentityWithRoles(testUser, Set.of()));
        params1.setDb(DB_1);
        TGetTablesResult result1 = frontendService.getTableNames(params1);

        Assertions.assertFalse(result1.getTables().contains(TABLE_1),
                "With SET ROLE NONE, should NOT see " + TABLE_1 + " in " + DB_1);

        // SET ROLE NONE: should not see tbl2 in db2
        TGetTablesParams params2 = new TGetTablesParams();
        params2.setCurrent_user_ident(buildUserIdentityWithRoles(testUser, Set.of()));
        params2.setDb(DB_2);
        TGetTablesResult result2 = frontendService.getTableNames(params2);

        Assertions.assertFalse(result2.getTables().contains(TABLE_2),
                "With SET ROLE NONE, should NOT see " + TABLE_2 + " in " + DB_2);
    }

    // -----------------------------------------------------------------------
    // Integration test: SET ROLE ALL with both roles shows everything
    // -----------------------------------------------------------------------

    @Test
    public void testSetRoleAllShowsAllDatabases() throws Exception {
        UserIdentity testUser = getTestUserIdentity();

        // Single role: limited visibility
        TGetDbsParams params1 = new TGetDbsParams();
        params1.setCurrent_user_ident(buildUserIdentityWithRoles(testUser, Set.of(role1Id)));
        TGetDbsResult result1 = frontendService.getDbNames(params1);

        // Both roles: full visibility
        TGetDbsParams params2 = new TGetDbsParams();
        params2.setCurrent_user_ident(buildUserIdentityWithRoles(testUser, Set.of(role1Id, role2Id)));
        TGetDbsResult result2 = frontendService.getDbNames(params2);

        Assertions.assertTrue(result2.getDbs().size() >= result1.getDbs().size(),
                "SET ROLE ALL (both roles) should show at least as many databases as single role");
        Assertions.assertTrue(result2.getDbs().contains(DB_1) && result2.getDbs().contains(DB_2),
                "With both roles, should see both databases");
    }
}
