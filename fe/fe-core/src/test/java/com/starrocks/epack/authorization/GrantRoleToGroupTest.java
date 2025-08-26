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

package com.starrocks.epack.authorization;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.GrantType;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.epack.persist.EditLogEPack;
import com.starrocks.epack.persist.OperationTypeEPack;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.UpdateGroupToRoleLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.RevokeRoleStmt;
import com.starrocks.sql.ast.ShowGrantsStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.transaction.MockedLocalMetaStore;
import com.starrocks.transaction.MockedMetadataMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

public class GrantRoleToGroupTest {

    @Test
    public void testAlterAndDrop() throws Exception {
        EditLog editLog = spy(new EditLogEPack(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        ConnectContext ctx = new ConnectContext();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        AuthorizationMgrEPack authorizationMgrEPack = new AuthorizationMgrEPack(new AuthorizationProviderEPack());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgrEPack);
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(new AuthenticationMgr());

        for (int i = 1; i <= 2; i++) {
            String sql = "create role r" + i;
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            DDLStmtExecutor.execute(stmt, ctx);
        }

        GrantRoleStmt grantRoleStmt;
        grantRoleStmt = new GrantRoleStmt(List.of("r1", "r2"), "g1", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgrEPack.grantRole(grantRoleStmt);
        grantRoleStmt = new GrantRoleStmt(List.of("r1"), "g2", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgrEPack.grantRole(grantRoleStmt);

        Long r1Id = authorizationMgrEPack.getRoleIdByNameAllowNull("r1");
        Long r2Id = authorizationMgrEPack.getRoleIdByNameAllowNull("r2");

        Set<Long> roleIds = authorizationMgrEPack.getRoleIdListByGroup("g1");
        Assert.assertEquals(2, roleIds.size());
        Assert.assertTrue(roleIds.contains(r1Id));
        Assert.assertTrue(roleIds.contains(r2Id));
        roleIds = authorizationMgrEPack.getRoleIdListByGroup("g2");
        Assert.assertEquals(1, roleIds.size());
        Assert.assertTrue(roleIds.contains(r1Id));
        roleIds = authorizationMgrEPack.getRoleIdListByGroup("g3");
        Assert.assertEquals(0, roleIds.size());

        RevokeRoleStmt revokeRoleStmt;
        revokeRoleStmt = new RevokeRoleStmt(List.of("r2"), "g1", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgrEPack.revokeRole(revokeRoleStmt);
        grantRoleStmt = new GrantRoleStmt(List.of("r1"), "g3", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgrEPack.grantRole(grantRoleStmt);

        roleIds = authorizationMgrEPack.getRoleIdListByGroup("g1");
        Assert.assertEquals(1, roleIds.size());
        Assert.assertTrue(roleIds.contains(r1Id));
        roleIds = authorizationMgrEPack.getRoleIdListByGroup("g2");
        Assert.assertEquals(1, roleIds.size());
        Assert.assertTrue(roleIds.contains(r1Id));
        roleIds = authorizationMgrEPack.getRoleIdListByGroup("g3");
        Assert.assertTrue(roleIds.contains(r1Id));
        Assert.assertEquals(1, roleIds.size());

        roleIds = authorizationMgrEPack.getRoleIdListByGroup("g4");
        Assert.assertEquals(0, roleIds.size());
    }

    @Test
    public void testSerDer() throws Exception {
        EditLog editLog = spy(new EditLogEPack(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        ConnectContext ctx = new ConnectContext();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        AuthorizationMgrEPack authorizationMgrEPack = new AuthorizationMgrEPack(new AuthorizationProviderEPack());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgrEPack);
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(new AuthenticationMgr());

        for (int i = 1; i <= 3; i++) {
            String sql = "create role r" + i;
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            DDLStmtExecutor.execute(stmt, ctx);
        }

        GrantRoleStmt grantRoleStmt;
        grantRoleStmt =
                new GrantRoleStmt(List.of("r1", "r2", "r3"), "g1", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgrEPack.grantRole(grantRoleStmt);
        grantRoleStmt = new GrantRoleStmt(List.of("r1"), "g2", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgrEPack.grantRole(grantRoleStmt);

        RevokeRoleStmt revokeRoleStmt;
        revokeRoleStmt = new RevokeRoleStmt(List.of("r3"), "g1", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgrEPack.revokeRole(revokeRoleStmt);

        String serialized = GsonUtils.GSON.toJson(authorizationMgrEPack);
        AuthorizationMgr newObject = GsonUtils.GSON.fromJson(serialized, AuthorizationMgr.class);

        Long r1Id = authorizationMgrEPack.getRoleIdByNameAllowNull("r1");
        Long r2Id = authorizationMgrEPack.getRoleIdByNameAllowNull("r2");

        Set<Long> roleIds = newObject.getRoleIdListByGroup("g1");
        Assert.assertEquals(2, roleIds.size());
        Assert.assertTrue(roleIds.contains(r1Id));
        Assert.assertTrue(roleIds.contains(r2Id));
        roleIds = newObject.getRoleIdListByGroup("g2");
        Assert.assertEquals(1, roleIds.size());
        Assert.assertTrue(roleIds.contains(r1Id));
        roleIds = newObject.getRoleIdListByGroup("g3");
        Assert.assertEquals(0, roleIds.size());
    }

    @Test
    public void testPersist() throws Exception {
        UtFrameUtils.setUpForPersistTest();

        ConnectContext ctx = new ConnectContext();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        AuthorizationMgrEPack authorizationMgrEPack = new AuthorizationMgrEPack(new AuthorizationProviderEPack());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgrEPack);
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(new AuthenticationMgr());

        for (int i = 1; i <= 3; i++) {
            String sql = "create role r" + i;
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            DDLStmtExecutor.execute(stmt, ctx);
        }

        Long r1Id = authorizationMgrEPack.getRoleIdByNameAllowNull("r1");
        Long r2Id = authorizationMgrEPack.getRoleIdByNameAllowNull("r2");
        String serialized = GsonUtils.GSON.toJson(authorizationMgrEPack);

        GrantRoleStmt grantRoleStmt;
        grantRoleStmt =
                new GrantRoleStmt(List.of("r1", "r2", "r3"), "g1", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgrEPack.grantRole(grantRoleStmt);
        grantRoleStmt = new GrantRoleStmt(List.of("r1"), "g2", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgrEPack.grantRole(grantRoleStmt);

        RevokeRoleStmt revokeRoleStmt;
        revokeRoleStmt = new RevokeRoleStmt(List.of("r3"), "g1", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgrEPack.revokeRole(revokeRoleStmt);

        AuthorizationMgr newObject = GsonUtils.GSON.fromJson(serialized, AuthorizationMgr.class);

        Set<Long> roleIds = newObject.getRoleIdListByGroup("g1");
        Assert.assertEquals(0, roleIds.size());
        roleIds = newObject.getRoleIdListByGroup("g2");
        Assert.assertEquals(0, roleIds.size());
        roleIds = newObject.getRoleIdListByGroup("g3");
        Assert.assertEquals(0, roleIds.size());

        UpdateGroupToRoleLog log1 = (UpdateGroupToRoleLog)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_GRANT_ROLE_TO_GROUP);
        newObject.replayGrantRoleToGroup(log1.getRoleIdList(), log1.getGroup());

        UpdateGroupToRoleLog log2 = (UpdateGroupToRoleLog)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_GRANT_ROLE_TO_GROUP);
        newObject.replayGrantRoleToGroup(log2.getRoleIdList(), log2.getGroup());

        UpdateGroupToRoleLog log3 = (UpdateGroupToRoleLog)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_REVOKE_ROLE_FROM_GROUP);
        newObject.replayRevokeRoleFromGroup(log3.getRoleIdList(), log3.getGroup());

        roleIds = newObject.getRoleIdListByGroup("g1");
        Assert.assertEquals(2, roleIds.size());
        Assert.assertTrue(roleIds.contains(r1Id));
        Assert.assertTrue(roleIds.contains(r2Id));
        roleIds = newObject.getRoleIdListByGroup("g2");
        Assert.assertEquals(1, roleIds.size());
        Assert.assertTrue(roleIds.contains(r1Id));
        roleIds = newObject.getRoleIdListByGroup("g3");
        Assert.assertEquals(0, roleIds.size());

        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testShowGrants() throws Exception {
        EditLog editLog = spy(new EditLogEPack(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        ConnectContext ctx = new ConnectContext();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        AuthorizationMgrEPack authorizationMgrEPack = new AuthorizationMgrEPack(new AuthorizationProviderEPack());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgrEPack);
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(new AuthenticationMgr());

        for (int i = 1; i <= 3; i++) {
            String sql = "create role r" + i;
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            DDLStmtExecutor.execute(stmt, ctx);
        }

        GrantRoleStmt grantRoleStmt;
        grantRoleStmt =
                new GrantRoleStmt(List.of("r1", "r2", "r3"), "g1", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgrEPack.grantRole(grantRoleStmt);
        grantRoleStmt = new GrantRoleStmt(List.of("r1"), "g2", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgrEPack.grantRole(grantRoleStmt);

        RevokeRoleStmt revokeRoleStmt;
        revokeRoleStmt = new RevokeRoleStmt(List.of("r3"), "g1", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgrEPack.revokeRole(revokeRoleStmt);

        ShowGrantsStmt stmt = new ShowGrantsStmt("g1", GrantType.GROUP, NodePosition.ZERO);
        ShowResultSet showResultSet = ShowExecutor.execute(stmt, ctx);
        Assert.assertEquals("[[g1, null, GRANT 'r1', 'r2' TO EXTERNAL GROUP g1]]", showResultSet.getResultRows().toString());

        stmt = new ShowGrantsStmt("g2", GrantType.GROUP, NodePosition.ZERO);
        showResultSet = ShowExecutor.execute(stmt, ctx);
        Assert.assertEquals("[[g2, null, GRANT 'r1' TO EXTERNAL GROUP g2]]", showResultSet.getResultRows().toString());
    }

    @Test
    public void testPrivilege() throws Exception {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        EditLog editLog = spy(new EditLogEPack(null));
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

        AuthorizationMgrEPack authorizationMgrEPack = new AuthorizationMgrEPack(new AuthorizationProviderEPack());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgrEPack);
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(new AuthenticationMgr());

        String createRoleSql = "create role r1";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(createRoleSql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);

        String createUserSql = "create user u1";
        stmt = UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);

        GrantRoleStmt grantRoleStmt = new GrantRoleStmt(List.of("r1"), "g1", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgrEPack.grantRole(grantRoleStmt);

        String sql = "grant select on table db1.tbl1 to role r1";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationMgrEPack.grant(grantStmt);

        ctx.setCurrentUserIdentity(new UserIdentity("u1", "%"));
        Assert.assertThrows(AccessDeniedException.class,
                () -> Authorizer.checkTableAction(ctx, "db1", "tbl1", PrivilegeType.SELECT));
        ctx.setGroups(Set.of("g1"));

        try {
            Authorizer.checkTableAction(ctx, "db1", "tbl1", PrivilegeType.SELECT);
        } catch (Exception e) {
            Assert.fail();
        }

        ctx.setGroups(Set.of());
        Assert.assertThrows(AccessDeniedException.class,
                () -> Authorizer.checkTableAction(ctx, "db1", "tbl1", PrivilegeType.SELECT));
    }
}
