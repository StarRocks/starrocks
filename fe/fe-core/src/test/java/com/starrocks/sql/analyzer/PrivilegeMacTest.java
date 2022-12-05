// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PrivilegeMacTest {
    private static StarRocksAssert starRocksAssert;
    private static UserIdentity testUser;
    private static UserIdentity testUser2;

    private static String role = null;
    private static Auth auth;
    private static UserIdentity rootUser;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        String createTblStmtStr = "create table db1.tbl1(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db1");
        starRocksAssert.withDatabase("db2");
        starRocksAssert.withTable(createTblStmtStr);
        auth = starRocksAssert.getCtx().getGlobalStateMgr().getAuth();
        rootUser = starRocksAssert.getCtx().getCurrentUserIdentity();
    }

    @Before
    public void beforeMethod() throws Exception {
        starRocksAssert.getCtx().setCurrentUserIdentity(rootUser);
        dropUsersAndRole();
        createUsersAndRole();
    }

    private void dropUsersAndRole() throws Exception {
        if (testUser != null) {
            auth.replayDropUser(testUser);
        }
        if (testUser2 != null) {
            auth.replayDropUser(testUser2);
        }

        if (role != null) {
            auth.dropRole((DropRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                    "DROP ROLE " + role, starRocksAssert.getCtx()));
            role = null;
        }
    }

    private void createUsersAndRole() throws Exception {
        String createUserSql = "CREATE USER 'test' IDENTIFIED BY ''";
        CreateUserStmt createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());
        auth.createUser(createUserStmt);

        testUser = createUserStmt.getUserIdent();

        createUserSql = "CREATE USER 'test2' IDENTIFIED BY ''";
        createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());
        auth.createUser(createUserStmt);

        testUser2 = createUserStmt.getUserIdent();

        role = "role0";
        auth.createRole((CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE ROLE " + role, starRocksAssert.getCtx()));
    }

    @Test
    public void testMacPrivilege() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        ctx.setCurrentUserIdentity(testUser);
        GrantPrivilegeStmt grantTable = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT SELECT_PRIV ON db1.tbl1 TO test", ctx);
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "alter table db1.tbl1 set (\"mac_access_label\" = \"top_secret\")", ctx);
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        globalStateMgr.getAuth().grant(grantTable);
        Database db1 = globalStateMgr.getDb("db1");
        OlapTable tbl1 = (OlapTable) db1.getTable("tbl1");
        globalStateMgr.alterTable(alterTableStmt);
        System.out.println(tbl1.getMacAccessLabel());

        String sql = "select k1 from db1.tbl1 as a";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        // has table privilege, but no mac privilege authorized
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));

        // authorize 'secret' mac privilege
        GrantPrivilegeStmt macGrant = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT SECRET ON mac_access_label TO test", ctx);
        globalStateMgr.getAuth().grant(macGrant);
        // mac level is not enough high
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));

        // authorize 'top_secret' mac privilege
        macGrant = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT TOP_SECRET ON mac_access_label TO test", ctx);
        globalStateMgr.getAuth().grant(macGrant);
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
        macGrant = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT TOP_SECRET ON MAC_ACCESS_LABEL TO test", ctx);
        globalStateMgr.getAuth().grant(macGrant);
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
    }

    @Test
    public void testMacPrivilegeNoTblPriv() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        ctx.setCurrentUserIdentity(testUser);
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "alter table db1.tbl1 set (\"mac_access_label\" = \"top_secret\")", ctx);
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db1 = globalStateMgr.getDb("db1");
        OlapTable tbl1 = (OlapTable) db1.getTable("tbl1");
        globalStateMgr.alterTable(alterTableStmt);
        System.out.println(tbl1.getMacAccessLabel());

        String sql = "select k1 from db1.tbl1 as a";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        // authorize 'secret' mac privilege
        GrantPrivilegeStmt macGrant = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT SECRET ON mac_access_label TO test", ctx);
        globalStateMgr.getAuth().grant(macGrant);
        // mac level is not enough high
        Assert.assertThrows(SemanticException.class,
                () -> PrivilegeChecker.check(statementBase, starRocksAssert.getCtx()));

        // authorize 'top_secret' mac privilege
        macGrant = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT TOP_SECRET ON mac_access_label TO test", ctx);
        globalStateMgr.getAuth().grant(macGrant);
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
        macGrant = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT TOP_SECRET ON MAC_ACCESS_LABEL TO test", ctx);
        globalStateMgr.getAuth().grant(macGrant);
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
    }

    @Test
    public void testShowTables() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        ctx.setCurrentUserIdentity(testUser);
        ctx.setDatabase("db1");
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser("show tables",
                starRocksAssert.getCtx());
        PrivilegeChecker.check(statementBase, starRocksAssert.getCtx());
    }

    @Test
    public void testMacPrivilegeView() throws Exception {
        starRocksAssert.withView("create view db1.view_mac (k3, k4) as select k3,k4 from db1.tbl1");
    }
}
