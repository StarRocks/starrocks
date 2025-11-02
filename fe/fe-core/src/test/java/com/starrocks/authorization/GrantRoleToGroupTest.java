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
import com.starrocks.common.ErrorReportException;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.UpdateGroupToRoleLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.GrantType;
import com.starrocks.sql.ast.RevokeRoleStmt;
import com.starrocks.sql.ast.ShowGrantsStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.transaction.MockedLocalMetaStore;
import com.starrocks.transaction.MockedMetadataMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

public class GrantRoleToGroupTest {

    @Test
    public void testAlterAndDrop() throws Exception {
        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        ConnectContext ctx = new ConnectContext();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        AuthorizationMgr authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(new AuthenticationMgr());

        for (int i = 1; i <= 2; i++) {
            String sql = "create role r" + i;
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            DDLStmtExecutor.execute(stmt, ctx);
        }

        GrantRoleStmt grantRoleStmt;
        grantRoleStmt = new GrantRoleStmt(List.of("r1", "r2"), "g1", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(grantRoleStmt, ctx);
        authorizationMgr.grantRole(grantRoleStmt);
        grantRoleStmt = new GrantRoleStmt(List.of("r1"), "g2", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(grantRoleStmt, ctx);
        authorizationMgr.grantRole(grantRoleStmt);

        Long r1Id = authorizationMgr.getRoleIdByNameAllowNull("r1");
        Long r2Id = authorizationMgr.getRoleIdByNameAllowNull("r2");

        Set<Long> roleIds = authorizationMgr.getRoleIdListByGroup("g1");
        Assertions.assertEquals(2, roleIds.size());
        Assertions.assertTrue(roleIds.contains(r1Id));
        Assertions.assertTrue(roleIds.contains(r2Id));
        roleIds = authorizationMgr.getRoleIdListByGroup("g2");
        Assertions.assertEquals(1, roleIds.size());
        Assertions.assertTrue(roleIds.contains(r1Id));
        roleIds = authorizationMgr.getRoleIdListByGroup("g3");
        Assertions.assertEquals(0, roleIds.size());

        RevokeRoleStmt revokeRoleStmt;
        revokeRoleStmt = new RevokeRoleStmt(List.of("r2"), "g1", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(grantRoleStmt, ctx);
        authorizationMgr.revokeRole(revokeRoleStmt);
        grantRoleStmt = new GrantRoleStmt(List.of("r1"), "g3", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(grantRoleStmt, ctx);
        authorizationMgr.grantRole(grantRoleStmt);

        roleIds = authorizationMgr.getRoleIdListByGroup("g1");
        Assertions.assertEquals(1, roleIds.size());
        Assertions.assertTrue(roleIds.contains(r1Id));
        roleIds = authorizationMgr.getRoleIdListByGroup("g2");
        Assertions.assertEquals(1, roleIds.size());
        Assertions.assertTrue(roleIds.contains(r1Id));
        roleIds = authorizationMgr.getRoleIdListByGroup("g3");
        Assertions.assertTrue(roleIds.contains(r1Id));
        Assertions.assertEquals(1, roleIds.size());

        roleIds = authorizationMgr.getRoleIdListByGroup("g4");
        Assertions.assertEquals(0, roleIds.size());
    }

    @Test
    public void testSerDer() throws Exception {
        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        ConnectContext ctx = new ConnectContext();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        AuthorizationMgr authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(new AuthenticationMgr());

        for (int i = 1; i <= 3; i++) {
            String sql = "create role r" + i;
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            DDLStmtExecutor.execute(stmt, ctx);
        }

        GrantRoleStmt grantRoleStmt;
        grantRoleStmt =
                new GrantRoleStmt(List.of("r1", "r2", "r3"), "g1", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgr.grantRole(grantRoleStmt);
        grantRoleStmt = new GrantRoleStmt(List.of("r1"), "g2", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgr.grantRole(grantRoleStmt);

        RevokeRoleStmt revokeRoleStmt;
        revokeRoleStmt = new RevokeRoleStmt(List.of("r3"), "g1", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgr.revokeRole(revokeRoleStmt);

        String serialized = GsonUtils.GSON.toJson(authorizationMgr);
        AuthorizationMgr newObject = GsonUtils.GSON.fromJson(serialized, AuthorizationMgr.class);

        Long r1Id = authorizationMgr.getRoleIdByNameAllowNull("r1");
        Long r2Id = authorizationMgr.getRoleIdByNameAllowNull("r2");

        Set<Long> roleIds = newObject.getRoleIdListByGroup("g1");
        Assertions.assertEquals(2, roleIds.size());
        Assertions.assertTrue(roleIds.contains(r1Id));
        Assertions.assertTrue(roleIds.contains(r2Id));
        roleIds = newObject.getRoleIdListByGroup("g2");
        Assertions.assertEquals(1, roleIds.size());
        Assertions.assertTrue(roleIds.contains(r1Id));
        roleIds = newObject.getRoleIdListByGroup("g3");
        Assertions.assertEquals(0, roleIds.size());
    }

    @Test
    public void testPersist() throws Exception {
        UtFrameUtils.setUpForPersistTest();

        ConnectContext ctx = new ConnectContext();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        AuthorizationMgr authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(new AuthenticationMgr());

        for (int i = 1; i <= 3; i++) {
            String sql = "create role r" + i;
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            DDLStmtExecutor.execute(stmt, ctx);
        }

        Long r1Id = authorizationMgr.getRoleIdByNameAllowNull("r1");
        Long r2Id = authorizationMgr.getRoleIdByNameAllowNull("r2");
        String serialized = GsonUtils.GSON.toJson(authorizationMgr);

        GrantRoleStmt grantRoleStmt;
        grantRoleStmt =
                new GrantRoleStmt(List.of("r1", "r2", "r3"), "g1", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgr.grantRole(grantRoleStmt);
        grantRoleStmt = new GrantRoleStmt(List.of("r1"), "g2", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgr.grantRole(grantRoleStmt);

        RevokeRoleStmt revokeRoleStmt;
        revokeRoleStmt = new RevokeRoleStmt(List.of("r3"), "g1", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgr.revokeRole(revokeRoleStmt);

        AuthorizationMgr newObject = GsonUtils.GSON.fromJson(serialized, AuthorizationMgr.class);

        Set<Long> roleIds = newObject.getRoleIdListByGroup("g1");
        Assertions.assertEquals(0, roleIds.size());
        roleIds = newObject.getRoleIdListByGroup("g2");
        Assertions.assertEquals(0, roleIds.size());
        roleIds = newObject.getRoleIdListByGroup("g3");
        Assertions.assertEquals(0, roleIds.size());

        UpdateGroupToRoleLog log1 = (UpdateGroupToRoleLog)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_GRANT_ROLE_TO_GROUP);
        newObject.replayGrantRoleToGroup(log1.getRoleIdList(), log1.getGroup());

        UpdateGroupToRoleLog log2 = (UpdateGroupToRoleLog)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_GRANT_ROLE_TO_GROUP);
        newObject.replayGrantRoleToGroup(log2.getRoleIdList(), log2.getGroup());

        UpdateGroupToRoleLog log3 = (UpdateGroupToRoleLog)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_REVOKE_ROLE_FROM_GROUP);
        newObject.replayRevokeRoleFromGroup(log3.getRoleIdList(), log3.getGroup());

        roleIds = newObject.getRoleIdListByGroup("g1");
        Assertions.assertEquals(2, roleIds.size());
        Assertions.assertTrue(roleIds.contains(r1Id));
        Assertions.assertTrue(roleIds.contains(r2Id));
        roleIds = newObject.getRoleIdListByGroup("g2");
        Assertions.assertEquals(1, roleIds.size());
        Assertions.assertTrue(roleIds.contains(r1Id));
        roleIds = newObject.getRoleIdListByGroup("g3");
        Assertions.assertEquals(0, roleIds.size());

        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testShowGrants() throws Exception {
        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        ConnectContext ctx = new ConnectContext();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        AuthorizationMgr authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(new AuthenticationMgr());

        for (int i = 1; i <= 3; i++) {
            String sql = "create role r" + i;
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            DDLStmtExecutor.execute(stmt, ctx);
        }

        GrantRoleStmt grantRoleStmt;
        grantRoleStmt =
                new GrantRoleStmt(List.of("r1", "r2", "r3"), "g1", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgr.grantRole(grantRoleStmt);
        grantRoleStmt = new GrantRoleStmt(List.of("r1"), "g2", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgr.grantRole(grantRoleStmt);

        RevokeRoleStmt revokeRoleStmt;
        revokeRoleStmt = new RevokeRoleStmt(List.of("r3"), "g1", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgr.revokeRole(revokeRoleStmt);

        ShowGrantsStmt stmt = new ShowGrantsStmt("g1", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(stmt, ctx);
        ShowResultSet showResultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals("[[g1, null, GRANT 'r1', 'r2' TO EXTERNAL GROUP g1]]", showResultSet.getResultRows().toString());

        stmt = new ShowGrantsStmt("g2", GrantType.GROUP, NodePosition.ZERO);
        showResultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals("[[g2, null, GRANT 'r1' TO EXTERNAL GROUP g2]]", showResultSet.getResultRows().toString());
    }

    @Test
    public void testPrivilege() throws Exception {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        MockedLocalMetaStore
                localMetastore = new MockedLocalMetaStore(globalStateMgr, globalStateMgr.getRecycleBin(), null);
        globalStateMgr.setLocalMetastore(localMetastore);

        MockedMetadataMgr mockedMetadataMgr = new MockedMetadataMgr(localMetastore, globalStateMgr.getConnectorMgr());
        globalStateMgr.setMetadataMgr(mockedMetadataMgr);

        localMetastore.createDb("db1");
        String createTable = "create table db1.tbl1 (c1 bigint, c2 bigint, c3 bigint)";
        CreateTableStmt createTableStmt =
                (CreateTableStmt) SqlParser.parseSingleStatement(createTable, ctx.getSessionVariable().getSqlMode());
        Analyzer.analyze(createTableStmt, ctx);
        localMetastore.createTable(createTableStmt);

        AuthorizationMgr authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(new AuthenticationMgr());

        String createRoleSql = "create role r1";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(createRoleSql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);

        String createUserSql = "create user u1";
        stmt = UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);

        GrantRoleStmt grantRoleStmt = new GrantRoleStmt(List.of("r1"), "g1", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(grantRoleStmt, ctx);
        authorizationMgr.grantRole(grantRoleStmt);

        String sql = "grant select on table db1.tbl1 to role r1";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationMgr.grant(grantStmt);

        ctx.setCurrentUserIdentity(new UserIdentity("u1", "%"));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> Authorizer.checkTableAction(ctx, "db1", "tbl1", PrivilegeType.SELECT));
        ctx.setGroups(Set.of("g1"));

        try {
            Authorizer.checkTableAction(ctx, "db1", "tbl1", PrivilegeType.SELECT);
        } catch (Exception e) {
            Assertions.fail();
        }

        ctx.setGroups(Set.of());
        Assertions.assertThrows(AccessDeniedException.class,
                () -> Authorizer.checkTableAction(ctx, "db1", "tbl1", PrivilegeType.SELECT));
    }

    @Test
    public void testShowGrantsPrivilege() throws Exception {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);
        ConnectContext ctx = new ConnectContext();

        String createUserSql = "create user u_grant";
        CreateUserStmt stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);
        ctx.setCurrentUserIdentity(new UserIdentity("u_grant", "%"));

        ctx.setGroups(Set.of("g1"));
        ShowGrantsStmt stmt2 = new ShowGrantsStmt("g1", GrantType.GROUP, NodePosition.ZERO);
        Authorizer.check(stmt2, ctx);
        ShowGrantsStmt stmt3 = new ShowGrantsStmt("g2", GrantType.GROUP, NodePosition.ZERO);
        Assertions.assertThrows(AccessDeniedException.class, () -> Authorizer.checkSystemAction(ctx, PrivilegeType.GRANT));
        Assertions.assertThrows(ErrorReportException.class, () -> Authorizer.check(stmt3, ctx));
    }

    @Test
    public void testShowGrantsForExternalGroup() throws Exception {
        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        ConnectContext ctx = new ConnectContext();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        AuthorizationMgr authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(new AuthenticationMgr());

        // Create roles
        for (int i = 1; i <= 4; i++) {
            String sql = "create role r" + i;
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            DDLStmtExecutor.execute(stmt, ctx);
        }

        // Test 1: Grant single role to external group and verify show grants
        GrantRoleStmt grantRoleStmt = new GrantRoleStmt(List.of("r1"), "test_group_1", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(grantRoleStmt, ctx);
        authorizationMgr.grantRole(grantRoleStmt);

        ShowGrantsStmt stmt = new ShowGrantsStmt("test_group_1", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(stmt, ctx);
        ShowResultSet showResultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals("[[test_group_1, null, GRANT 'r1' TO EXTERNAL GROUP test_group_1]]",
                showResultSet.getResultRows().toString());

        // Test 2: Grant multiple roles to external group and verify show grants
        grantRoleStmt = new GrantRoleStmt(List.of("r2", "r3", "r4"), "test_group_2", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(grantRoleStmt, ctx);
        authorizationMgr.grantRole(grantRoleStmt);

        stmt = new ShowGrantsStmt("test_group_2", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(stmt, ctx);
        showResultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals("[[test_group_2, null, GRANT 'r2', 'r3', 'r4' TO EXTERNAL GROUP test_group_2]]",
                showResultSet.getResultRows().toString());

        // Test 3: Grant additional roles to existing group and verify show grants
        grantRoleStmt = new GrantRoleStmt(List.of("r3", "r4"), "test_group_1", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(grantRoleStmt, ctx);
        authorizationMgr.grantRole(grantRoleStmt);

        stmt = new ShowGrantsStmt("test_group_1", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(stmt, ctx);
        showResultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals("[[test_group_1, null, GRANT 'r1', 'r3', 'r4' TO EXTERNAL GROUP test_group_1]]",
                showResultSet.getResultRows().toString());

        // Test 4: Revoke some roles and verify show grants reflects the changes
        RevokeRoleStmt revokeRoleStmt = new RevokeRoleStmt(List.of("r3"), "test_group_1", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(revokeRoleStmt, ctx);
        authorizationMgr.revokeRole(revokeRoleStmt);

        stmt = new ShowGrantsStmt("test_group_1", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(stmt, ctx);
        showResultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals("[[test_group_1, null, GRANT 'r1', 'r4' TO EXTERNAL GROUP test_group_1]]",
                showResultSet.getResultRows().toString());

        // Test 5: Revoke all roles and verify show grants shows empty result
        revokeRoleStmt = new RevokeRoleStmt(List.of("r1", "r4"), "test_group_1", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(revokeRoleStmt, ctx);
        authorizationMgr.revokeRole(revokeRoleStmt);

        stmt = new ShowGrantsStmt("test_group_1", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(stmt, ctx);
        showResultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals("[]", showResultSet.getResultRows().toString());

        // Test 6: Verify that test_group_2 still has its roles (isolation test)
        stmt = new ShowGrantsStmt("test_group_2", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(stmt, ctx);
        showResultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals("[[test_group_2, null, GRANT 'r2', 'r3', 'r4' TO EXTERNAL GROUP test_group_2]]",
                showResultSet.getResultRows().toString());

        // Test 7: Test non-existent group shows empty result
        stmt = new ShowGrantsStmt("non_existent_group", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(stmt, ctx);
        showResultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals("[]", showResultSet.getResultRows().toString());
    }

    @Test
    public void testGrantAndRevokeExternalGroup() throws Exception {
        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        ConnectContext ctx = new ConnectContext();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        AuthorizationMgr authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(new AuthenticationMgr());

        // Create roles
        for (int i = 1; i <= 3; i++) {
            String sql = "create role r" + i;
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            DDLStmtExecutor.execute(stmt, ctx);
        }

        Long r1Id = authorizationMgr.getRoleIdByNameAllowNull("r1");
        Long r2Id = authorizationMgr.getRoleIdByNameAllowNull("r2");
        Long r3Id = authorizationMgr.getRoleIdByNameAllowNull("r3");

        // Test 1: Grant multiple roles to external group
        GrantRoleStmt grantRoleStmt =
                new GrantRoleStmt(List.of("r1", "r2", "r3"), "external_group_1", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(grantRoleStmt, ctx);
        authorizationMgr.grantRole(grantRoleStmt);

        // Verify roles are granted
        Set<Long> roleIds = authorizationMgr.getRoleIdListByGroup("external_group_1");
        Assertions.assertEquals(3, roleIds.size());
        Assertions.assertTrue(roleIds.contains(r1Id));
        Assertions.assertTrue(roleIds.contains(r2Id));
        Assertions.assertTrue(roleIds.contains(r3Id));

        // Test 2: Revoke one role from external group
        RevokeRoleStmt revokeRoleStmt = new RevokeRoleStmt(List.of("r2"), "external_group_1", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(revokeRoleStmt, ctx);
        authorizationMgr.revokeRole(revokeRoleStmt);

        // Verify revocation takes effect immediately
        roleIds = authorizationMgr.getRoleIdListByGroup("external_group_1");
        Assertions.assertEquals(2, roleIds.size());
        Assertions.assertTrue(roleIds.contains(r1Id));
        Assertions.assertFalse(roleIds.contains(r2Id)); // r2 should be revoked
        Assertions.assertTrue(roleIds.contains(r3Id));

        // Test 3: Revoke multiple roles at once
        revokeRoleStmt = new RevokeRoleStmt(List.of("r1", "r3"), "external_group_1", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(revokeRoleStmt, ctx);
        authorizationMgr.revokeRole(revokeRoleStmt);

        // Verify all roles are revoked
        roleIds = authorizationMgr.getRoleIdListByGroup("external_group_1");
        Assertions.assertEquals(0, roleIds.size());

        // Test 4: Grant role to another external group and verify isolation
        grantRoleStmt = new GrantRoleStmt(List.of("r1", "r2"), "external_group_2", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(grantRoleStmt, ctx);
        authorizationMgr.grantRole(grantRoleStmt);

        // Verify external_group_1 is still empty
        roleIds = authorizationMgr.getRoleIdListByGroup("external_group_1");
        Assertions.assertEquals(0, roleIds.size());

        // Verify external_group_2 has the roles
        roleIds = authorizationMgr.getRoleIdListByGroup("external_group_2");
        Assertions.assertEquals(2, roleIds.size());
        Assertions.assertTrue(roleIds.contains(r1Id));
        Assertions.assertTrue(roleIds.contains(r2Id));

        // Test 5: Revoke from external_group_2 and verify effect
        revokeRoleStmt = new RevokeRoleStmt(List.of("r1"), "external_group_2", GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(revokeRoleStmt, ctx);
        authorizationMgr.revokeRole(revokeRoleStmt);

        roleIds = authorizationMgr.getRoleIdListByGroup("external_group_2");
        Assertions.assertEquals(1, roleIds.size());
        Assertions.assertFalse(roleIds.contains(r1Id)); // r1 should be revoked
        Assertions.assertTrue(roleIds.contains(r2Id)); // r2 should still be there
    }

    @Test
    public void testGrantRoleStmtParser() {
        String sql = "grant r1 to external group g1";
        StatementBase stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertInstanceOf(GrantRoleStmt.class, stmt);
        GrantRoleStmt grantRoleStmt = (GrantRoleStmt) stmt;
        Assertions.assertEquals(List.of("r1"), grantRoleStmt.getGranteeRole());
        Assertions.assertEquals("g1", grantRoleStmt.getRoleOrGroup());
        Assertions.assertEquals(GrantType.GROUP, grantRoleStmt.getGrantType());

        sql = "revoke r1 from external group g1";
        stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertInstanceOf(RevokeRoleStmt.class, stmt);
        RevokeRoleStmt revokeRoleStmt = (RevokeRoleStmt) stmt;
        Assertions.assertEquals(List.of("r1"), revokeRoleStmt.getGranteeRole());
        Assertions.assertEquals("g1", revokeRoleStmt.getRoleOrGroup());
        Assertions.assertEquals(GrantType.GROUP, revokeRoleStmt.getGrantType());

        sql = "show grants for external group g1";
        stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertInstanceOf(ShowGrantsStmt.class, stmt);
        ShowGrantsStmt showGrantsStmt = (ShowGrantsStmt) stmt;
        Assertions.assertEquals("g1", showGrantsStmt.getGroupOrRole());
        Assertions.assertEquals(GrantType.GROUP, showGrantsStmt.getGrantType());
    }
}
